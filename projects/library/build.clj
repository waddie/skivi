(ns build
  (:require [clojure.string :as str]
            [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

(def lib 'dev.skivi/skivi)
(def version (or (System/getenv "VERSION") "0.0.0-SNAPSHOT"))
(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))

(def src-dirs
  (->> (:classpath-roots basis)
       (map str)
       (filter #(str/ends-with? % "/src"))
       vec))

(def resource-dirs
  (->> (:classpath-roots basis)
       (map str)
       (filter #(str/ends-with? % "/resources"))
       vec))

(def jar-file (str "target/skivi-" version ".jar"))
(def uber-file (str "target/skivi-" version "-uber.jar"))

(defn clean [_] (b/delete {:path "target"}))

(defn prep
  [_]
  (b/write-pom
   {:basis     basis
    :class-dir class-dir
    :lib       lib
    :pom-data  [[:description "A Clojure-based job queue system for PostgreSQL"]
                [:url "https://skivi.dev/"]
                [:licenses
                 [:license [:name "GNU Affero General Public License Version 3"]
                  [:url "https://www.gnu.org/licenses/agpl-3.0.en.html"]]
                 [:license [:name "Commercial"]
                  [:url "https://skivi.dev/licensing.html"]]]]
    :scm       {:connection "scm:git:git://github.com/waddie/skivi.git"
                :developerConnection
                "scm:git:ssh://git@github.com/waddie/skivi.git"
                :tag        (str "v" version)
                :url        "https://github.com/waddie/skivi"}
    :src-dirs  src-dirs
    :version   version})
  (b/copy-dir {:src-dirs   (into src-dirs resource-dirs)
               :target-dir class-dir}))

(def opts
  {:basis     basis
   :class-dir class-dir})

(defn jar
  [_]
  (b/compile-clj {:basis     basis
                  :class-dir class-dir
                  :src-dirs  src-dirs})
  (b/jar (merge opts {:jar-file jar-file})))

(defn uber
  [_]
  (b/compile-clj {:basis     basis
                  :class-dir class-dir
                  :src-dirs  src-dirs})
  (b/uber (merge opts {:uber-file uber-file})))

(defn tag-release
  [{:keys [version]}]
  (let [tag (str "v" version)]
    (b/git-process {:git-args ["tag" "-a" tag "-m" (str "Release " version)]})
    (b/git-process {:git-args ["push" "origin" tag]})))

(defn jar-all [_] (clean nil) (prep nil) (jar nil))

(defn uber-all [_] (clean nil) (prep nil) (uber nil))

(defn jar-install
  [_]
  (b/install {:basis     basis
              :class-dir class-dir
              :jar-file  jar-file
              :lib       lib
              :version   version}))

(defn deploy
  [_]
  (dd/deploy {:artifact  jar-file
              :installer :remote
              :pom-file  (b/pom-path {:class-dir class-dir
                                      :lib       lib})}))
