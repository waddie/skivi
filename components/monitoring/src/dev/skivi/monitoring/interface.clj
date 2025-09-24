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

(ns dev.skivi.monitoring.interface
  "Event emitter with fan-out delivery, a ring buffer, and operational stats.

  Emitters are plain maps with atom-backed state. Create one per system
  and pass it to components that need to emit events (worker-pool, queue, etc.).

  Typical usage:

    ;; Create from the :events section of MonitoringConfig
    (def emitter (monitoring/create-emitter {:enabled true :buffer-size 500}))

    ;; Register a structured logging handler for all events
    (monitoring/on emitter :all
                   (fn [{:keys [type data]}]
                     (log/info \"event\" (assoc data :event/type type))))

    ;; Register a handler for a specific event type
    (def hid (monitoring/on emitter :job/claimed
                             (fn [event] (metrics/increment! :jobs-claimed))))

    ;; Emit from a component
    (monitoring/emit! emitter :job/claimed {:job-id id :worker-id wid})

    ;; Deregister when no longer needed
    (monitoring/off emitter hid)

    ;; Inspect recent events (ring buffer snapshot)
    (monitoring/events emitter)

    ;; In tests: use noop-emitter to silence all monitoring
    (monitoring/noop-emitter)

    ;; In tests: use collecting-emitter to assert events were emitted
    (let [em (monitoring/collecting-emitter)]
      (do-something em)
      (is (some #(= :job/claimed (:type %)) (monitoring/events em))))

  Standard event types emitted by skivi components:
    :job/claimed         - worker claimed a job from the database
    :job/completed       - worker completed a job successfully
    :job/failed          - worker failed a job (retry eligible)
    :job/exhausted       - worker failed a job at max_attempts
    :job/partial-success - worker reported partial success
    :queue/locked        - a named job queue was locked by a worker
    :queue/unlocked      - a named job queue was released
    :cron/fired          - a cron tab entry scheduled a job
    :worker/error        - an unhandled exception occurred in a worker
    :pool/start          - the worker pool started
    :pool/stop           - the worker pool stopped"
  (:require [dev.skivi.monitoring.core :as core]
            [dev.skivi.monitoring.schema :as schema]))

(defn create-emitter
  "Creates an event emitter from an events config map.
  config keys: :enabled (boolean), :buffer-size (pos-int).
  Called with no args creates an emitter with sensible defaults."
  {:malli/schema [:function
                  [:=> [:cat] schema/Emitter]
                  [:=> [:cat schema/EmitterConfig] schema/Emitter]]}
  ([] (core/create-emitter))
  ([config] (core/create-emitter config)))

(defn noop-emitter
  "Returns a disabled emitter. All emit! calls and handler registrations are
  no-ops. Use when monitoring is disabled in config or in unit tests."
  {:malli/schema [:function [:=> [:cat] schema/Emitter]]}
  []
  (core/noop-emitter))

(defn collecting-emitter
  "Returns an emitter that buffers every event without dropping.
  Call events to retrieve all emitted events. Use in tests."
  {:malli/schema [:function [:=> [:cat] schema/Emitter]]}
  []
  (core/collecting-emitter))

(defn enabled?
  "Returns true if the emitter will route events to handlers."
  {:malli/schema [:function [:=> [:cat schema/Emitter] :boolean]]}
  [emitter]
  (core/enabled? emitter))

(defn on
  "Registers handler-fn to receive events of event-type. Pass :all to receive
  every event. Returns a handler-id (UUID) for deregistration via off.
  Returns nil when the emitter is disabled."
  {:malli/schema [:function
                  [:=> [:cat schema/Emitter :keyword ifn?] [:maybe :uuid]]]}
  [emitter event-type handler-fn]
  (core/on emitter event-type handler-fn))

(defn off
  "Removes the handler identified by handler-id. Returns nil."
  {:malli/schema [:function [:=> [:cat schema/Emitter :uuid] :nil]]}
  [emitter handler-id]
  (core/off emitter handler-id))

(defn emit!
  "Emits an event of event-type with data map to all registered handlers.
  Exceptions thrown by handlers are caught and counted as :dropped.
  Returns nil. No-op when emitter is disabled."
  {:malli/schema [:function
                  [:=> [:cat schema/Emitter :keyword :map] :nil]]}
  [emitter event-type data]
  (core/emit! emitter event-type data))

(defn events
  "Returns a snapshot of buffered events in emission order (oldest first).
  Each event is a map with :type, :data, and :emitted-at keys."
  {:malli/schema [:function [:=> [:cat schema/Emitter] [:vector schema/Event]]]}
  [emitter]
  (core/events emitter))

(defn stats
  "Returns a snapshot of emitter operational metrics.
  Keys: :emitted (total events emitted), :dropped (handler errors caught)."
  {:malli/schema [:function [:=> [:cat schema/Emitter] schema/EmitterStats]]}
  [emitter]
  (core/stats emitter))
