# Advanced example: order processing

This example builds on the [basic example](../basic/). Read that first if you
haven’t – it covers dependency setup, task handlers, config files, and the
system life-cycle.

Here we have an order processing system. When a customer places an order,
we need to charge their payment method and send a confirmation email. A few
real-world complications make this more involved than a welcome email:

- **Double-submits happen.** A flaky mobile client or an eager user might POST
  the same order twice. We should charge them once.
- **Payment processors impose rate limits.** We need to stay under the limit
  without dropping jobs or hammering a wait loop.
- **Payment and confirmation should succeed or fail together.** We don’t want
  an order that’s charged with no confirmation, or confirmed before the charge
  goes through.
- **Serial payment processing.** Payments run through a named queue, ensuring
  they’re processed one at a time.
- **Nightly reconciliation.** At 2 AM, we reconcile the day’s orders with the
  payment processor.
- **Alerting on exhausted jobs.** If a payment job burns through all its retry
  attempts, we want to know immediately.

The code is in `src/orders/`.

## Deduplication with job keys

The `process-payment` job uses the order ID as its `:job-key`, with `:unsafe-dedupe` mode:

```clojure
{:task-identifier "process-payment"
 :payload         {:order-id "order-123" ...}
 :job-key         "order-123"
 :job-key-mode    :unsafe-dedupe}
```

If a job with that key already exists in the queue, the second enqueue does
nothing. `:unsafe-dedupe` doesn’t check whether the existing job has the same
payload – it only cares whether any job with that key is pending. That’s fine
here: we don’t want to charge the customer again regardless of what changed.

The other modes are worth knowing. `:replace` atomically deletes the existing
job and inserts a fresh one – useful when you want to reset retry state or
update the payload. `:preserve-run-at` updates the job’s parameters while
keeping its scheduled execution time – useful when you’re debouncing: you want
the job to run at the originally scheduled time, but with the latest data.

## Named queues

Assigning `:queue-name "payments"` serialises jobs through that queue: only one
payment runs at a time, regardless of how many workers you have. While a payment
job is in flight, the others wait.

```clojure
{:task-identifier "process-payment"
 :queue-name      "payments"
 ...}
```

The queue is created automatically the first time a job uses it. Named queues
are a good fit when the resource you’re accessing can’t handle concurrent
operations, or when ordering matters. They do impose some serialisation overhead
though, so don’t use them when you don’t need it.

## Rate limiting

Rate limits are registered against the connection pool after starting the
system. The `database` namespace from Skivi’s internals exposes the call:

```clojure
(ns orders.core
  (:require [dev.skivi.database.interface :as database]
            ...))

(defn- setup-rate-limits [sys]
  ;; 100 calls per minute to the payment processor.
  (database/register-rate-limit (:pool sys) "payment-processor" 100 "1 minute")
  sys)
```

Each payment job then declares `:rate-limit-key "payment-processor"`. When a
worker claims the job, it consumes a token. When the bucket is empty, workers
defer remaining jobs to the next refill window rather than retrying immediately.

Rate limiting and named queues are orthogonal. A job can use both, either, or neither.

## Atomic batch enqueue

`add-jobs` inserts a sequence of job specs in a single database transaction.
Either all of them are queued or none are:

```clojure
(skivi/add-jobs system
  [{:task-identifier "process-payment"  :payload {...} ...}
   {:task-identifier "send-confirmation" :payload {...}}])
```

This is meaningfully different from two separate `add-job` calls wrapped in a
`try`. With `add-jobs`, a failure during the insert means neither job exists.
With two separate calls, you can end up with a payment job queued and no
confirmation, or vice versa.

## Payload validation

Specs are defined in `src/orders/specs.clj` using `clojure.spec.alpha`, then
referenced by keyword in the config. Skivi detects whether a `:job-schemas`
value is a malli schema (a vector) or a spec keyword, and validates accordingly.

```edn
;; resources/skivi-config.edn
{...
 :schema {:name              "skivi"
          :validate-payloads true
          :job-schemas       {:process-payment   :orders.specs/process-payment-payload
                              :send-confirmation :orders.specs/send-confirmation-payload
                              :reconcile-daily   :orders.specs/reconcile-daily-payload}}}
```

The `orders.specs` namespace is required (without an alias) in `core.clj` so
the specs are registered with the spec registry before the system starts:

```clojure
(ns orders.core
  (:require [orders.specs]
            ...))
```

Skivi validates the payload before it touches the database. A bad payload raises
an error at the call site – in your request handler, where you can return a
useful response – rather than inside the worker, where the job might retry
several times before anyone notices.

A common mistake is passing a decimal amount instead of integer pence:

```clojure
(orders/place-order! {:id             "order-123"
                      :customer-id    "cust-456"
                      :email          "alice@example.com"
                      :amount         49.99   ;; wrong — should be integer pence
                      :items          [{:name "Widget" :qty 1}]
                      :payment-method "pm_test_xxxx"})
;; Throws ExceptionInfo "Invalid payload"
;; {:task-identifier "process-payment"
;;  :payload         {:amount 49.99, :customer-id "cust-456", ...}
;;  :errors          #:clojure.spec.alpha
;;                   {:problems ({:path [:amount]
;;                                :pred clojure.core/int?
;;                                :val  49.99
;;                                :via  [:orders.specs/process-payment-payload
;;                                       :orders.specs/amount]
;;                                :in   [:amount]})
;;                    :spec  :orders.specs/process-payment-payload
;;                    :value {:amount 49.99, ...}}}
```

The exception is thrown by `place-order!` before anything touches the database,
so there is nothing to roll back and no job to retry.

## Cron scheduling

The nightly reconciliation is defined in `src/orders/cron.clj`:

```clojure
(def tabs
  [{:identifier "reconcile-daily"
    :schedule   "0 2 * * *"
    :spec       {:queue-name  "maintenance"
                 :max-attempts 3}}])
```

Pass `cron/tabs` as the third argument to `create-system`. Enable the scheduler
in `skivi-config.edn`:

```edn
:cron {:enabled                true
       :file                   "crontab.edn"
       :timezone               "UTC"
       :default-backfill-period 3600000}
```

Skivi persists the last-execution timestamp to the database, so the scheduler
survives process restarts without double-firing. If the previous run’s job
is still queued or in flight when the next tick arrives, the new enqueue is
silently skipped – this is the `unsafe_dedupe` behaviour, applied automatically
to cron jobs.

## Monitoring

The event emitter fires on job lifecycle events. Handlers are attached after `start!`:

```clojure
(ns orders.core
  (:require [dev.skivi.monitoring.interface :as monitoring]
            ...))

(defn- setup-monitoring [sys]
  (monitoring/on (:emitter sys) :job/exhausted
    (fn [{:keys [data]}]
      (when (= "process-payment" (:task-identifier data))
        (alert! "Payment exhausted" (get-in data [:payload :order-id])))))
  sys)
```

Attach to `:all` for structured logging:

```clojure
(monitoring/on (:emitter sys) :all
  (fn [{:keys [type data]}]
    (log/info "skivi" {:event/type type :data data})))
```

Standard event types: `:job/claimed`, `:job/completed`, `:job/failed`,
`:job/exhausted`, `:job/partial-success`, `:pool/start`, `:pool/stop`,
`:cron/fired`, `:worker/error`.

## Running the example

If you don’t have a local PostgreSQL instance, start one with Docker:

```bash
docker compose up -d
```

This starts a `postgres:17` container with an `orders_db` database on
the default port, no password required. The config file falls back to
`jdbc:postgresql://localhost:5432/orders_db?user=postgres` when `DATABASE_URL`
isn’t set, which matches the Docker setup. If your database is elsewhere,
set `DATABASE_URL` beforehand.

Start a REPL from this directory:

```bash
clojure -M
```

Start the system first, then place an order:

```clojure
(require '[orders.core :as orders])
(orders/start!)
;; => starts migrations, worker pool, scheduler, and registers the rate limit

(orders/place-order! {:id             "order-123"
                      :customer-id    "cust-456"
                      :email          "alice@example.com"
                      :amount         4999
                      :items          [{:name "Widget" :qty 1}]
                      :payment-method "pm_test_xxxx"})
```

You should see both the payment and confirmation handlers execute. Try calling
`place-order!` again with the same order ID – the payment job is silently
deduplicated, and only the confirmation is enqueued again.

To see the rate limiter in action, enqueue a burst of orders. The first 100
will be claimed immediately; subsequent jobs will be deferred to the next minute
window.
