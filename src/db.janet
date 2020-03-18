(import ./db/helper :prefix "")
(import ./db/migrator :as migrator)

(def database-url (os/getenv "DATABASE_URL"))

# conditional imports
(if (and database-url
         (string/has-prefix? "postgres" database-url))
  (import ./db/pg :prefix "" :export true)
  (import ./db/sqlite3 :prefix "" :export true))


(defn new
  "Creates migrations and databases"
  [command & args]
  (case command
    "database" (file/touch (first args))
    "database:sqlite" (file/touch (first args))
    "database:postgres" (os/shell (string "createdb " (first args)))
    "migration" (migrator/create-migration (first args))
    "table" (migrator/create-table-migration args)))


(defn migrate [])
(defn rollback [])
(def version "0.1.0")
