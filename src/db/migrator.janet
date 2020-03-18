(import path)
(import ./helper :prefix "")


(defn timestamp
  "Generate a timestamp for migration files"
  []
  (let [date (os/date)
        M (+ 1 (date :month))
        D (+ 1 (date :month-day))
        Y (date :year)
        HH (date :hours)
        MM (date :minutes)
        SS (date :seconds)]
    (string/format "%d%.2d%.2d%.2d%.2d%.2d" Y M D HH MM SS)))


(defn create-migration [name &opt content]
  (default content {})
  (os/mkdir "db")
  (os/mkdir (path/join "db" "migrations"))
  (when (string? name)
    (let [{:up up :down down} content
          filename (string (timestamp) "-" name ".sql")
          filename (path/join "db" "migrations" filename)
          contents (string "-- up\n" (or up "") "\n\n-- down\n" (or down ""))]
      (file/write-all filename contents))))


(defn create-table-migration [args]
  (let [table-name (snake-case (first args))
        columns (-> (apply array (drop 1 args))
                    (array/insert 0 "id integer primary key")
                    (array/push "created_at integer not null default(strftime('%s', 'now'))")
                    (array/push "updated_at integer"))
        columns-sql (string/join columns ",\n  ")]
    (create-migration (string "create-table-" table-name)
     {:up (string/format "create table %s (\n  %s\n)" table-name columns-sql)
      :down (string/format "drop table %s" table-name)})))
