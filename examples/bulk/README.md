# Bulk example: 3,000 Fibonacci jobs

This example enqueues 3,000 independent jobs and runs them twice — once at
full throughput, once with a rate limit — to show how Skivi handles large
workloads and how rate limiting changes worker behaviour.

Each job computes one number from the Fibonacci sequence and writes it to a
file. The sequence numbers 0 and 1 are skipped; jobs start at F(3) = 1:

| Job | Sequence position | File       | Content |
| --- | ----------------- | ---------- | ------- |
| 1   | 3                 | `output/3` | `1`     |
| 2   | 4                 | `output/4` | `2`     |
| 3   | 5                 | `output/5` | `3`     |
| 4   | 6                 | `output/6` | `5`     |
| …   | …                 | …          | …       |

The fast run writes to `output/fast/`; the throttled run writes to
`output/throttled/`. Both directories are created automatically.

Read the [basic example](../basic/) first if you haven't — it covers
dependency setup, task handlers, config files, and the system life-cycle.

## What to expect

### Fast run

All 20 workers claim jobs immediately and run at full speed. The progress
listener prints a line every 1,000 completions so you can watch throughput.

The Fibonacci numbers grow to roughly 630 digits by F(3002), so all jobs finish
quickly. The full run typically completes in under a minute.

### Throttled run

The rate limiter caps claims at 600 jobs per minute. Once the initial burst of
600 is claimed, workers drain their in-flight jobs and then go idle until the
token bucket refills at the start of the next minute window. You can observe
`(fibonacci.core/stats)` returning `:active 0` during the idle phase, then
jumping back up when the window resets.

With 3,000 jobs at 600/minute the throttled run takes around 5 minutes to
complete — long enough to clearly observe the burst-and-pause cycle.

## Prerequisites

- Clojure CLI tools
- PostgreSQL running locally, or Docker to start one

## Running the example

Start PostgreSQL:

```sh
docker compose up -d
```

Start a REPL from this directory:

```sh
clojure -M
```

### Run 1: no rate limiting

```clojure
(require '[fibonacci.core :as fib])

(fib/start!)
(fib/run-fast!)
```

You should see output like:

```
Enqueueing 3,000 jobs → output/fast
Enqueued in 1.4s. Workers are processing...
    1,000 / 3,000  (312 jobs/s, 3.2s elapsed)
    2,000 / 3,000  (298 jobs/s, 6.7s elapsed)
    3,000 / 3,000  (285 jobs/s, 10.5s elapsed)
```

Enqueuing 3k jobs in 500-row batches takes a second or two against a local
database. Workers start claiming as soon as the first batch lands.

### Run 2: rate limited

Wait until the fast run is complete (or stop it first), then:

```clojure
(fib/run-throttled!)
```

The first 600 jobs are claimed in a burst, then workers idle until the next
minute. You can observe the cycle:

```clojure
;; While a burst is in progress
(fib/stats)
;; => {:active 20, :completed 543, :failed 0, :errors 0}

;; During the idle phase
(fib/stats)
;; => {:active 0, :completed 600, :failed 0, :errors 0}
```

### Stopping

```clojure
(fib/stop!)
```

`stop!` drains in-flight jobs and closes the connection pool. Jobs that were
enqueued but not yet claimed remain in the database; they will be picked up if
you call `start!` again.

## How it works

### Enqueueing in batches

Inserting 3,000 rows one at a time would open 3,000 transactions. Instead,
`run-fast!` and `run-throttled!` use `skivi/add-jobs` in batches of 500, each
as a single transaction:

```clojure
(doseq [batch (partition-all 500 job-specs)]
  (skivi/add-jobs sys (vec batch)))
```

Workers start claiming the first batch while the remaining batches are still
being inserted.

### Rate limiting

Rate limits are registered once by name, then referenced on individual jobs:

```clojure
;; Register the limit (called once, idempotent on re-registration)
(skivi/register-rate-limit system "fibonacci-throttle" 600 "1 minute")

;; Reference it on each job spec
{:task-identifier "write-fibonacci"
 :payload         {:n 42 :output-dir "output/throttled"}
 :rate-limit-key  "fibonacci-throttle"}
```

The token bucket lives in the database, so the limit is shared across all
workers — and across multiple application instances, if you were running
several. Unthrottled jobs (like those in the fast run) are never affected by
a rate limit they don't reference.

### Computing Fibonacci

Each job independently iterates from F(1) through F(n) using BigInteger
arithmetic:

```clojure
(defn- fib [n]
  (loop [a 0N b 1N i 1]
    (if (= i n) a
      (recur b (+' a b) (inc i)))))
```

Jobs are independent — no shared state, no coordination between workers. That
is what makes them a good fit for a job queue.
