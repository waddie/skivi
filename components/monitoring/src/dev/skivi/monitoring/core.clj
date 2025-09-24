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

(ns dev.skivi.monitoring.core
  "Event emitter: fan-out delivery to registered handlers with a ring buffer.

  Handlers registered with :all receive every event regardless of type.
  Handlers registered with a specific keyword receive only that event type.
  Handler errors are caught, counted in :dropped stats, and never propagate.

  Thread safety: all mutable state lives in atoms updated with swap!/reset!.
  emit! and handler registration are safe to call concurrently."
  (:require [dev.skivi.monitoring.schema :as schema])
  (:import [java.time Instant]))

(defn- build-emitter
  [enabled? config]
  {:buffer   (atom [])
   :config   config
   :enabled? enabled?
   :handlers (atom {})
   :stats    (atom {:dropped 0
                    :emitted 0})})

(defn create-emitter
  "Creates an event emitter from an events config map.
  config keys: :enabled (boolean), :buffer-size (pos-int)."
  {:malli/schema [:function
                  [:=> [:cat] schema/Emitter]
                  [:=> [:cat schema/EmitterConfig] schema/Emitter]]}
  ([]
   (create-emitter {:buffer-size 1000
                    :enabled     true}))
  ([config] (build-emitter (get config :enabled true) config)))

(defn noop-emitter
  "Returns a disabled emitter. All emit! calls and handler registrations are
  no-ops. Use in tests or when monitoring is turned off in config."
  {:malli/schema [:function [:=> [:cat] schema/Emitter]]}
  []
  (build-emitter false
                 {:buffer-size 0
                  :enabled     false}))

(defn collecting-emitter
  "Returns an emitter that buffers every event without dropping.
  Useful in tests: call events to retrieve what was emitted."
  {:malli/schema [:function [:=> [:cat] schema/Emitter]]}
  []
  (build-emitter true
                 {:buffer-size Integer/MAX_VALUE
                  :enabled     true}))

(defn enabled?
  "Returns true if the emitter will route events to handlers."
  {:malli/schema [:function [:=> [:cat schema/Emitter] :boolean]]}
  [emitter]
  (boolean (:enabled? emitter)))

(defn on
  "Registers handler-fn to receive events of event-type. Pass :all to receive
  every event regardless of type. Returns a handler-id (UUID) that can be passed
  to off to deregister. Returns nil when the emitter is disabled."
  {:malli/schema [:function
                  [:=> [:cat schema/Emitter :keyword ifn?] [:maybe :uuid]]]}
  [emitter event-type handler-fn]
  (when (:enabled? emitter)
    (let [handler-id (random-uuid)]
      (swap! (:handlers emitter) update event-type assoc handler-id handler-fn)
      handler-id)))

(defn off
  "Removes the handler identified by handler-id from emitter. Returns nil."
  {:malli/schema [:function [:=> [:cat schema/Emitter :uuid] :nil]]}
  [emitter handler-id]
  (swap! (:handlers emitter)
    (fn [all]
      (reduce-kv
       (fn [acc evt handlers-for-evt]
         (let [trimmed (dissoc handlers-for-evt handler-id)]
           (if (empty? trimmed) (dissoc acc evt) (assoc acc evt trimmed))))
       {}
       all)))
  nil)

(defn- add-to-ring-buffer
  "Appends event to buf, dropping the oldest entry when at capacity."
  [buf event capacity]
  (if (>= (count buf) capacity) (conj (subvec buf 1) event) (conj buf event)))

(defn emit!
  "Emits an event of event-type with data map. Delivers to all :all handlers
  then all handlers registered for event-type. Exceptions thrown by handlers are
  caught and counted as :dropped. Returns nil."
  {:malli/schema [:function
                  [:=> [:cat schema/Emitter :keyword :map] :nil]]}
  [emitter event-type data]
  (when (:enabled? emitter)
    (let [event    {:data       data
                    :emitted-at (Instant/now)
                    :type       event-type}
          capacity (get-in emitter [:config :buffer-size] 1000)
          hmap     @(:handlers emitter)
          handlers (merge (get hmap :all) (get hmap event-type))]
      (swap! (:stats emitter) update :emitted inc)
      (swap! (:buffer emitter) add-to-ring-buffer event capacity)
      (doseq [[_ handler-fn] handlers]
        (try (handler-fn event)
             (catch Exception _
               (swap! (:stats emitter) update :dropped inc))))))
  nil)

(defn events
  "Returns a snapshot of buffered events in emission order (oldest first)."
  {:malli/schema [:function [:=> [:cat schema/Emitter] [:vector schema/Event]]]}
  [emitter]
  @(:buffer emitter))

(defn stats
  "Returns a snapshot of emitter operational metrics.
  Keys: :emitted (total events emitted), :dropped (handler errors caught)."
  {:malli/schema [:function [:=> [:cat schema/Emitter] schema/EmitterStats]]}
  [emitter]
  @(:stats emitter))
