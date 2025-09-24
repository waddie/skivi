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

(ns dev.skivi.job-manager.core
  (:require [dev.skivi.database.interface :as db]
            [dev.skivi.job-manager.schema :as schema]
            [dev.skivi.validation.interface :as validation])
  (:import [java.time Instant]))

(defn- backoff-delay-ms
  "Computes retry delay: base-delay * multiplier^min(attempts, 10),
  clamped to [base-delay, max-delay], with up to (jitter * 100)% random variance."
  [attempts {:keys [base-delay max-delay multiplier jitter]}]
  (let [raw       (* (double base-delay)
                     (Math/pow (double multiplier) (min attempts 10)))
        clamped   (min (double max-delay) (max (double base-delay) raw))
        jitter-ms (* clamped (double jitter) (rand))]
    (long (+ clamped jitter-ms))))

;;; JobEnqueue surface

(defn add-job
  "Validates payload then adds single job to queue."
  {:malli/schema [:function
                  [:=> [:cat schema/JobManagerSystem :string :map] schema/Job]
                  [:=> [:cat schema/JobManagerSystem :string :map :map]
                   schema/Job]]}
  ([system task-identifier payload] (add-job system task-identifier payload {}))
  ([system task-identifier payload opts]
   (validation/validate-payload (:validator system) task-identifier payload)
   (db/add-job (:pool system) task-identifier payload opts)))

(defn add-jobs
  "Validates all payloads then adds multiple jobs atomically."
  {:malli/schema [:function
                  [:=> [:cat schema/JobManagerSystem [:vector schema/JobSpec]]
                   [:vector schema/Job]]]}
  [system job-specs]
  (doseq [{:keys [task-identifier payload]} job-specs]
    (validation/validate-payload (:validator system) task-identifier payload))
  (db/add-jobs (:pool system) job-specs))

(defn reschedule-jobs
  "Updates run-at, priority, or max-attempts for jobs."
  {:malli/schema [:function
                  [:=> [:cat schema/JobManagerSystem [:vector :uuid] :map]
                   [:vector schema/Job]]]}
  [system job-ids opts]
  (db/reschedule-jobs (:pool system) job-ids opts))

(defn replay-failed-jobs
  "Creates new jobs from failed history records matching criteria."
  {:malli/schema [:function
                  [:=> [:cat schema/JobManagerSystem :map]
                   [:vector schema/Job]]]}
  [system criteria]
  (db/replay-failed-jobs (:pool system) criteria))

(defn permanently-fail-jobs
  "Sets jobs to exhausted status with reason. Unlocks any queues affected."
  {:malli/schema [:function
                  [:=> [:cat schema/JobManagerSystem [:vector :uuid] :string]
                   [:vector schema/Job]]]}
  [system job-ids reason]
  (db/permanently-fail-jobs (:pool system) job-ids reason))

(defn force-unlock-jobs
  "Resets locked_by and locked_at on locked jobs. Unlocks affected queues.
  Optionally restricted to given worker-ids; nil means all locked jobs."
  {:malli/schema [:function
                  [:=> [:cat schema/JobManagerSystem] [:vector schema/Job]]
                  [:=> [:cat schema/JobManagerSystem [:maybe [:vector :string]]]
                   [:vector schema/Job]]]}
  ([system] (force-unlock-jobs system nil))
  ([system worker-ids] (db/force-unlock-jobs (:pool system) worker-ids)))

(defn force-unlock-queues
  "Clears locked_by and locked_at on job queues.
  Optionally restricted to given queue-names; nil means all locked queues."
  {:malli/schema [:function
                  [:=> [:cat schema/JobManagerSystem] [:vector :map]]
                  [:=> [:cat schema/JobManagerSystem [:maybe [:vector :string]]]
                   [:vector :map]]]}
  ([system] (force-unlock-queues system nil))
  ([system queue-names] (db/force-unlock-queues (:pool system) queue-names)))

(defn gc-job-queues
  "Removes empty, unlocked job queues."
  {:malli/schema [:function [:=> [:cat schema/JobManagerSystem] :int]]}
  [system]
  (db/gc-job-queues (:pool system)))

;;; WorkerExecution surface

(defn get-jobs
  "Retrieves and locks available jobs for worker-id. Generates a correlation-id
  per job and records job start in history. Returns jobs with schema/correlation-id-key
  embedded for use with complete-jobs, fail-jobs, and report-partial-success."
  {:malli/schema
   [:function
    [:=> [:cat schema/JobManagerSystem :string] [:vector schema/Job]]
    [:=> [:cat schema/JobManagerSystem :string :map] [:vector schema/Job]]]}
  ([system worker-id] (get-jobs system worker-id {}))
  ([system worker-id opts]
   (mapv (fn [job]
           (-> job
               (assoc schema/correlation-id-key (:correlation-id job))
               (dissoc :correlation-id)))
         (db/get-jobs (:pool system) worker-id opts))))

(defn complete-jobs
  "Marks jobs as completed and updates their history records.
  jobs must be the enriched maps returned by get-jobs."
  {:malli/schema
   [:function
    [:=> [:cat schema/JobManagerSystem :string [:vector schema/Job] :int]
     :nil]]}
  [system worker-id jobs execution-time-ms]
  (let [pool    (:pool system)
        job-ids (mapv :id jobs)]
    (db/complete-jobs pool worker-id job-ids)
    (doseq [job jobs]
      (when-let [cid (schema/correlation-id-key job)]
        (db/record-job-completion pool
                                  (:id job)
                                  worker-id
                                  cid
                                  execution-time-ms
                                  nil)))))

(defn fail-jobs
  "Records job failures: reschedules with exponential backoff and updates history.
  job-errors is a sequence of {:job job :error throwable-or-string} maps.
  jobs must be the enriched maps returned by get-jobs."
  {:malli/schema
   [:function
    [:=> [:cat schema/JobManagerSystem :string [:vector schema/JobError] :int]
     :nil]]}
  [system worker-id job-errors execution-time-ms]
  (let [pool      (:pool system)
        retry-cfg (:retry-config system)
        now-ms    (.toEpochMilli (Instant/now))
        failures  (mapv (fn [{:keys [job error]}]
                          {:error-message   (if (instance? Throwable error)
                                              (ex-message error)
                                              (str error))
                           :job-id          (:id job)
                           :next-run-at     (when retry-cfg
                                              (Instant/ofEpochMilli
                                               (+ now-ms
                                                  (backoff-delay-ms
                                                   (:attempts job)
                                                   retry-cfg))))
                           :target-attempts (:attempts job)})
                        job-errors)]
    (db/fail-jobs pool worker-id failures)
    (doseq [{:keys [job error]} job-errors]
      (when-let [cid (schema/correlation-id-key job)]
        (db/record-job-failure pool
                               (:id job)
                               worker-id
                               cid
                               execution-time-ms
                               error)))))

(defn report-partial-success
  "Records partial success: reschedules job with exponential backoff and records
  partial_success in history. job must be the enriched map returned by get-jobs.
  When attempts = max-attempts, routes to permanent exhaustion instead of rescheduling
  to prevent the job becoming a zombie (rescheduled but never claimable)."
  {:malli/schema [:function
                  [:=>
                   [:cat schema/JobManagerSystem :string schema/Job
                    schema/PartialResults :int]
                   :nil]]}
  [system worker-id job partial-results execution-time-ms]
  (let [pool       (:pool system)
        retry-cfg  (:retry-config system)
        now-ms     (.toEpochMilli (Instant/now))
        exhausted? (>= (:attempts job) (:max-attempts job))]
    (if exhausted?
      (db/permanently-fail-jobs pool
                                [(:id job)]
                                "max-attempts reached on partial-success")
      (db/fail-jobs pool
                    worker-id
                    [{:error-message nil
                      :job-id        (:id job)
                      :next-run-at   (when retry-cfg
                                       (Instant/ofEpochMilli
                                        (+ now-ms
                                           (backoff-delay-ms (:attempts job)
                                                             retry-cfg))))}]))
    (when-let [cid (schema/correlation-id-key job)]
      (db/record-partial-success pool
                                 (:id job)
                                 worker-id
                                 cid
                                 execution-time-ms
                                 partial-results))
    nil))

;;; Maintenance

(defn reset-locked-jobs
  "Resets jobs whose locks have exceeded the timeout. Returns count of reset jobs."
  {:malli/schema [:function
                  [:=> [:cat schema/JobManagerSystem] :int]
                  [:=> [:cat schema/JobManagerSystem :map] :int]]}
  ([system] (reset-locked-jobs system {}))
  ([system opts] (db/reset-locked-jobs (:pool system) opts)))

(defn gc-task-identifiers
  "Removes unused task identifiers. Returns count of deleted rows."
  {:malli/schema [:function
                  [:=> [:cat schema/JobManagerSystem] :int]
                  [:=> [:cat schema/JobManagerSystem :map] :int]]}
  ([system] (gc-task-identifiers system {}))
  ([system opts] (db/gc-task-identifiers (:pool system) opts)))

(defn gc-job-history
  "Removes expired job history records. Returns count of deleted rows."
  {:malli/schema [:function [:=> [:cat schema/JobManagerSystem] :int]]}
  [system]
  (db/gc-job-history (:pool system)))

;;; Rate limits

(defn register-rate-limit
  "Registers or updates a rate limit. capacity is max tokens per interval.
  interval is a SQL interval string (e.g. \"1 minute\")."
  {:malli/schema [:function
                  [:=> [:cat schema/JobManagerSystem :string pos-int? :string]
                   :any]]}
  [system key capacity interval]
  (db/register-rate-limit (:pool system) key capacity interval))

(defn refill-rate-limits
  "Refills tokens for all rate limits whose window has expired."
  {:malli/schema [:function [:=> [:cat schema/JobManagerSystem] :any]]}
  [system]
  (db/refill-rate-limits (:pool system)))
