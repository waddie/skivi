(ns user
  #_{:clj-kondo/ignore [:unused-namespace :unused-referred-var]}
  (:require [cider.nrepl :refer [cider-nrepl-handler]]
            [clojure.pprint :refer [pprint]]
            [dev.skivi.config.interface :as config]
            [honey.sql :as sql]
            [honey.sql.helpers :as h]
            [malli.dev :as md]
            [malli.dev.pretty :as mdp]
            [nrepl.server :as nrepl]
            [still.core :refer [snap snap!]]))

(set! *warn-on-reflection* true)

(comment
  (defonce server (nrepl/start-server cider-nrepl-handler)))

(try (snap ::load-config (config/load-config))
     (catch Exception e
       (do (println (str (ex-message e) ":")) (pprint (:errors (ex-data e)))))
     (finally (println "Done")))

(comment
  (md/start! {:report (mdp/reporter (mdp/-printer {:colors       false
                                                   :print-length 30
                                                   :print-level  2
                                                   :print-meta   false
                                                   :width        80}))}))
