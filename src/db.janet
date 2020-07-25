(import ./db/helper :prefix "" :export true)
(import ./db/migrator :prefix "" :export true)
(import ./db/core :prefix "" :export true)

(def version "0.3.0")

(defn new
  "Creates migrations and databases"
  [command & args]
  (case command
    "database" (file/touch (first args))
    "database:sqlite" (file/touch (first args))
    "database:postgres" (os/shell (string "createdb " (first args)))
    "migration" (create-migration (first args))
    "table" (create-table-migration args)
    "migrate" (migrate)
    "rollback" (rollback)))
