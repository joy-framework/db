(import path)
(import ./helper :prefix "")
(import ./core :as db)


(defn- created-at []
  (if sqlite?
    "created_at integer not null default(strftime('%s', 'now'))"
    "created_at timestamptz not null default(now())"))


(defn- updated-at []
  (if sqlite?
    "updated_at integer"
    "updated_at timestamptz"))


(defn- primary-key []
  (if sqlite?
    "id integer primary key"
    "id serial primary key"))


(defn- timestamp
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
                    (array/insert 0 (primary-key))
                    (array/push (created-at))
                    (array/push (updated-at)))
        columns-sql (string/join columns ",\n  ")]
    (create-migration (string "create-table-" table-name)
     {:up (string/format "create table %s (\n  %s\n)" table-name columns-sql)
      :down (string/format "drop table %s" table-name)})))


(def- up-token "-- up")
(def- down-token "-- down")
(def- migrations-dir "db/migrations")


(defn- parse-migration [sql]
  (let [parts (string/split "\n" sql)
        up-index (find-index |(= $ up-token) parts)
        down-index (find-index |(= $ down-token) parts)
        up-sql (-> (array/slice parts (inc up-index) down-index)
                   (string/join "\n"))
        down-sql (-> (array/slice parts (inc down-index) -1)
                     (string/join "\n"))]
    {:up up-sql
     :down down-sql}))


(defn- file-migration-map []
  (as-> (os/dir migrations-dir) ?
       (filter |(string/has-suffix? ".sql" $) ?)
       (mapcat |(tuple (-> (string/split "-" $)
                           (first))
                       $) ?)
       (apply struct ?)))


(defn- db-versions []
  (->> (db/query "select version from schema_migrations order by version")
       (map |(get $ :version))))


(defn- pending-migrations [db-versions file-migration-map]
  (let [versions (->> (array/concat @[] (keys file-migration-map) db-versions)
                      (frequencies)
                      (pairs)
                      (filter (fn [[_ v]] (= v 1)))
                      (map first)
                      (sort))]
    (map |(get file-migration-map $) versions)))


(defn migrate [&opt db-url]
  (db/connect db-url)

  (db/with-transaction
    (db/execute "create table if not exists schema_migrations (version text primary key)")
    (let [migrations (pending-migrations (db-versions) (file-migration-map))]
      (loop [migration :in migrations]
        (let [version (-> (string/split "-" migration)
                          (first))
              filename (path/join migrations-dir migration)
              up (as-> filename ?
                       (file/read-all ?)
                       (parse-migration ?)
                       (get ? :up))]
          (print "Migrating [" migration "]...")
          (print up)
          (db/execute up)
          (db/execute "insert into schema_migrations (version) values (:version)" {:version version})
          (when (not= "production" (os/getenv "JOY_ENV"))
            (db/write-schema-file))
          (print "Successfully migrated [" migration "]")))))

  (db/disconnect))


(defn rollback [&opt db-url]
  (db/connect db-url)

  (db/with-transaction
    (db/execute "create table if not exists schema_migrations (version text primary key)")
    (when-let [versions (db-versions)
               version (get (reverse versions) 0)
               migration (get (file-migration-map) version)
               filename (string migrations-dir "/" migration)
               down (as-> filename ?
                          (file/read-all ?)
                          (parse-migration ?)
                          (get ? :down))]
      (print "Rolling back [" migration "]...")
      (print down)
      (db/execute down)
      (db/execute "delete from schema_migrations where version = :version" {:version version})
      (when (not= "production" (os/getenv "JOY_ENV"))
        (db/write-schema-file))
      (print "Successfully rolled back [" migration "]")))

  (db/disconnect))
