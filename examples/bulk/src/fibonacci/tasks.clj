(ns fibonacci.tasks
  (:require [babashka.fs :as fs]))

(defn- fib
  "Returns the nth Fibonacci number (1-indexed: F(1)=0, F(2)=1, F(3)=1, ...)."
  [n]
  (loop [a 0N
         b 1N
         i 1]
    (if (= i n) a (recur b (+' a b) (inc i)))))

(defn write-fibonacci
  [{:keys [job]}]
  (let [{:keys [n output-dir]} (:payload job)
        result (fib n)
        f      (fs/file output-dir (str n))]
    (fs/create-dirs (fs/parent f))
    (spit f (str result))))

(def registry {"write-fibonacci" write-fibonacci})
