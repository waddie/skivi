;; Copyright (c) 2025-2026 Tom Waddington
;
;;
;; This software is dual-licensed:
;;
;; 1. GNU Affero General Public License v3.0 or later (AGPL-3.0-or-later)
;;    This program is free software: you can redistribute it and/or modify
;;    it under the terms of the GNU Affero General Public License as published
;;    by the Free Software Foundation, either version 3 of the License, or
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

(ns dev.skivi.migration.core
  (:require [clojure.tools.logging :as log]
            [dev.skivi.migration.schema :as schema]
            [migratus.core :as migratus]))

(defn- migratus-config
  [{:keys [connection-string schema-name]}]
  ;; Do NOT set currentSchema here: migratus creates its schema_migrations
  ;; tracking table before running any migrations, so the target schema
  ;; doesn't exist yet. The migration SQL uses ${migratus.schema}
  ;; substitution so all objects land in the configured schema, not a
  ;; hardcoded one.
  {:db            {:connection-uri connection-string}
   :migration-dir "migrations"
   :properties    {:map {:migratus {:schema (or schema-name "skivi")}}}
   :store         :database})

(defn migrate!
  "Apply all pending migrations to the target database."
  {:malli/schema [:function [:=> [:cat schema/MigrationConfig] :nil]]}
  [config]
  (log/info "Applying migrations")
  (migratus/migrate (migratus-config config)))

(defn rollback!
  "Roll back the most recently applied migration."
  {:malli/schema [:function [:=> [:cat schema/MigrationConfig] :nil]]}
  [config]
  (log/info "Rolling back migration")
  (migratus/rollback (migratus-config config)))
