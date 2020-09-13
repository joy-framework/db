(import sqlite3)
(import ./sql :as sql)
(import ../helper :prefix "")


(defn connect [&opt url]
  (default url database-url)

  (unless url
    (error "DATABASE_URL environment variable isn't set"))

  (setdyn :db/connection (sqlite3/open url))

  (let [db (dyn :db/connection)]
    (sqlite3/eval db "PRAGMA foreign_keys=1;")
    db))


(defn disconnect []
  (sqlite3/close (dyn :db/connection))
  (setdyn :db/connection nil))


(defmacro with-connection
  [& body]
  ~(do
     (,connect)
     ,;body
     (,disconnect)))


(defmacro with-transaction
  `A macro that wraps database statements in a transaction`
  [& body]
  ~(try
     (do
       (,sqlite3/eval (,dyn :db/connection) "BEGIN TRANSACTION;")
       ,;body
       (,sqlite3/eval (,dyn :db/connection) "COMMIT;"))
     ([err fib]
      (,sqlite3/eval (,dyn :db/connection) "ROLLBACK;")
      (propagate err fib))))


(defn query
  `Executes a query against a sqlite database.

  Example:

  (import db)

  (db/query "select * from todos")

  # or

  (db/query "select * from todos where id = :id" {:id 1})

  => [{:id 1 :name "name"} {...} ...]`
  [sql &opt params table-name]
  (default params {})
  (let [params (snake-case-keys params)
        db (dyn :db/connection)]
    (as-> (sqlite3/eval db (string sql ";") params) ?
          (map kebab-case-keys ?)
          (map |(if table-name
                  (merge $ {:db/table (keyword table-name)})
                  $)
               ?))))


(defn query2 [sql-vec table-name]
  (let [sql (-> sql-vec first (string ";"))
        params (drop 1 sql-vec)]

    (->> (sqlite3/eval (dyn :db/connection) sql params)
         (map kebab-case-keys)
         (map |(put $ :db/table (keyword table-name))))))


(defn execute
  `Executes a query against a sqlite database.

  The first arg is the sql to execute, the second optional arg is a dictionary
  for any values you want to pass in.

  Example:

  (import db)

  (db/execute "create table todo (id integer primary key, name text)")

  # or

  (db/execute "insert into todo (id, name) values (:id, :name)" {:id 1 :name "name"})

  => Returns the last inserted row id, in this case 1`
  [sql &opt params]
  (default params {})
  (let [params (snake-case-keys params)
        db (dyn :db/connection)]
    (sqlite3/eval db (string sql ";") params)
    (sqlite3/last-insert-rowid db)))


(defn execute2 [sql-vec]
  (let [db (dyn :db/connection)
        sql (-> sql-vec first (string ";"))
        params (drop 1 sql-vec)]

    (sqlite3/eval db sql params)
    (sqlite3/last-insert-rowid db)))


(defn last-inserted
  `Takes a row id and returns the first record in the table that matches
  and returns the last inserted row from the rowid. Returns nil if a
  row for the rowid doesn't exist.

  Example:

  (import db)

  (db/last-inserted "todo" 1)

  => {:id 1 :name "name"}`
  [table-name rowid]
  (-> (sql/from table-name {:where {:rowid rowid} :limit 1})
      (query2 table-name)
      (first)))


(def- schema-sql
  `select
    m.name as tbl,
    m.name || '.' || pti.name as col
  from sqlite_master m
  join pragma_table_info(m.name) pti on m.name != pti.name`)

(defn schema []
  (as-> schema-sql ?
        (query ?)
        (group-by |($ :tbl) ?)
        (map-vals |(map (fn [x] (x :col)) $) ?)))


(defn fetch
  `Takes a path into the db and optional args
   and returns the first row that matches or nil if none exists.

  Example:

  (import db)

  (db/fetch [:todo 1])

  => {:id 1 :name "name"}`
  [path & args]
  (let [table-name (->> path (filter keyword?) last)]

    (-> (sql/fetch path (put (table ;args) :limit 1))
        (query2 table-name)
        (first))))


(defn fetch-all
  `Takes a path into the db and optional args
   and returns all of the rows that match or an empty array if
   no rows match.

  Example:

  (import db)

  (db/fetch-all [:todo 1 :tag] :order "tag_name asc")

  (db/fetch-all [:todo 1] :limit 1 :order "tag_name desc")

  => [{:id 1 :tag-name "tag1"} {:id 2 :tag-name "tag2"}]`
  [path & args]
  (let [table-name (->> path (filter keyword?) last)]

    (-> (sql/fetch path (struct ;args))
        (query2 table-name))))


(defn- tupleize [val]
  (if (indexed? val)
    val
    [val]))


(defn- janetize [s]
  (-> (string/replace "." "/" s)
      (kebab-case)
      (keyword)))


(defn- strip-prefix [pfx s]
  (-> (string/replace (string pfx "/") "" s)
      (keyword)))


(defn- join-columns [join-table schema]
  (->> (get schema (snake-case join-table))
       (map janetize)))


(defn- join-dict [row join-columns join-key db-table]
  (as-> (table/slice row join-columns) ?
        (map-keys |(strip-prefix db-table $) ?)
        (put ? :db/table db-table)))


(defn- join-one-row [row args schema]
  (var output @{})

  (let [join-tables (->> (get args :join/one [])
                         (tupleize))]

    (loop [join-table :in join-tables]

      (let [join-columns (join-columns join-table schema)

            join-key (-> join-table kebab-case singular keyword)

            join-dict (join-dict row join-columns join-key join-key)

            row (-> (put row join-key join-dict)
                    (dissoc ;join-columns))]

        (set output (merge output row)))))

  output)


(defn- join-one-rows [rows args schema]
  (if (get args :join/one)
    (map |(join-one-row $ args schema) rows)
    rows))


(defn- join-many-rows [rows args schema]
  (if (or (nil? (get args :join/many))
          (empty? (get args :join/many)))
    rows

    (do
      (var output @{})

      (let [join-tables (->> (get args :join/many)
                             (tupleize))

            table-name (-> rows first (get :db/table))

            table-columns (as-> (get schema (snake-case table-name)) ?
                                (array/push ? :db/table)
                                (map |(->> $ janetize (strip-prefix table-name)) ?))

            table-row (table/slice (first rows) table-columns)]

        (loop [join-table :in join-tables]

          (let [join-columns (join-columns join-table schema)

                join-key (-> join-table kebab-case plural keyword)

                join-table (-> join-table kebab-case keyword)

                join-dicts (->> (map |(join-dict $ join-columns join-key join-table) rows)
                                (map freeze)
                                (distinct)
                                (map |(merge-into @{} $)))

                row (-> (put table-row join-key join-dicts)
                        (dissoc ;join-columns))]

            (set output (merge output row))))

       @[output]))))


(defn from
  `Takes a table name and optional args
   and returns all of the rows that match the query
   or an empty array if no rows match.

  Example:

  (import db)

  (db/from :todo :where {:completed true} :order "name" :limit 2)

  # or

  (db/from :todo :where {:completed true} :order "name desc" :limit 10)

  # or

  (db/from :todo :where ["completed = ?" 1] :order "name desc" :limit 10)

  => [{:id 1 name "name" :completed true} {:id 1 :name "name2" :completed true}]`
  [table-name & args]
  (let [args (struct ;args)
        schema (schema)]
    (-> (sql/from table-name args schema)
        (query2 table-name)
        (join-one-rows args schema)
        (join-many-rows args schema))))


(defn find-by
  `Takes a table name and optional args
   and returns either nil or the first row from the query.

  Example:

  (import db)

  (db/find-by :todo :where {:completed true} :order "name")

  # or

  (db/find-by :todo :where {:completed true} :order "name desc")

  => {:id 1 name "name" :completed true}`
  [table-name & args]
  (first (from table-name ;args)))


(defn find
  `Takes a table name and optional args
   and returns either nil or the first row by primary key.

  Example:

  (import db)

  (db/find :todo 1)

  => {:id 1 name "name" :completed true}`
  [table-name id]
  (-> (sql/from table-name {:where {:id id} :limit 1})
      (query2 table-name)
      (first)))


(defn insert
  `Takes a table name and a dictionary,
  inserts the dictionary as rows/columns into the database
  and returns the inserted row from the database.

  Example:

  (import db)

  (db/insert :todo {:name "name3"})

  # or

  (db/insert {:db/table :todo :name "name3"})

  => @{:id 3 :name "name3" :completed false}`
  [& args]
  (var table-name nil)
  (var params nil)

  (if (= 2 (length args))
    (do
      (set table-name (args 0))
      (set params (args 1)))
    (do
      (set table-name (snake-case (get (args 0) :db/table)))
      (set params (put (table ;(kvs (args 0))) :db/table nil))))

  (->> (sql/insert table-name params)
       (execute2)
       (last-inserted table-name)))


(defn insert-all
  `Takes an optional db connection, a table name and an array of dictionaries,
   inserts the array into the database and returns the inserted rows.
   All keys must be the same, as it only insert into one table at a time.

  Example:

  (import db)

  (db/insert-all :todo [{:name "name4"} {:name "name5"}])

  => @[@{:id 4 :name "name4" :completed false} @{:id 5 :name "name5" :completed false}]`
  [table-name ind]
  (->> (sql/insert-all table-name ind)
       (execute2))

  (reverse
    (from table-name
          :order "rowid desc"
          :limit (length ind))))


(defn- get-id [val]
  (if (dictionary? val)
    (get val :id)
    val))


(defn- put-updated-at [table-name set-params]
  (let [schema (schema)]
    (if (and (dictionary? schema)
             (find-index |(= $ "updated_at") (get schema (snake-case table-name))))
      (merge set-params {:updated-at (os/time)})
      set-params)))


(defn update
  `Takes a table name and a dictionary with an :id key OR an id value,
  and a dictionary with the new columns/values to be updated, updates the row in the
  database and returns the updated row.

  Example:

  (import db)

  (db/update :todo 4 {:name "new name 4"})

  # or

  (db/update :todo {:id 4} {:name "new name 4"})

  # or

  (db/update {:db/table :todo :id 4 :name "new name 4"})

  # or

  (db/update {:db/table :todo :id 4} {:name "new name 4"})

  => @{:id 4 :name "new name 4" :completed false}`
  [& args]

  (var table-name nil)
  (var dict-or-id nil)
  (var params nil)

  (if (= 3 (length args))
    (do
      (set table-name (args 0))
      (set dict-or-id (args 1))
      (set params (args 2)))
    (if (= 2 (length args))
      (do
        (set table-name (get (args 0) :db/table))
        (set dict-or-id (args 0))
        (set params (args 1)))
      (do
        (set table-name (get (args 0) :db/table))
        (set dict-or-id (get (args 0) :id))
        (-> (set params (merge-into @{} (args 0)))
            (put :id nil)
            (put :db/table nil)))))

  (let [sql-table-name (snake-case table-name)
        params (table ;(kvs params))
        id (get-id dict-or-id)
        sql-vec (sql/update table-name (put-updated-at table-name params) {:id id})]

    (execute2 sql-vec)
    (fetch [table-name id])))


(defn update-all
  `Takes a table name a dictionary representing the where clause
   and a dictionary representing the set clause and updates the rows in the
   database and returns them.

  Example:

  (import db)

  (db/update-all :todo :set {:completed false} :where {:completed true})

  => @[@{:id 1 :completed true} ...]`
  [table-name & args]
  (let [{:set set-params :where where-params} (struct ;args)
        ids (->> (from table-name :where where-params)
                 (map |(table/slice $ [:id]))
                 (mapcat values))
        sql-vec (sql/update-all table-name (put-updated-at table-name set-params) where-params)]

    (execute2 sql-vec)
    (from table-name :where {:id ids})))


(defn delete
  `Takes a table name, a dictionary with an :id key or an id value
   representing the primary key integer row in the database, executes a DELETE and
   returns the deleted row.

  Example:

  (import db)

  (db/delete :todo {:id 1})

  # or

  (db/delete :todo 1)

  # or

  (db/delete {:db/table :todo :id 1})

  => @{:id 1 :name "name" :completed true}`
  [& args]
  (var table-name nil)
  (var dict-or-id nil)

  (if (= 2 (length args))
    (do
      (set table-name (args 0))
      (set dict-or-id (args 1)))
    (do
      (set table-name (get (args 0) :db/table))
      (set dict-or-id (args 0))))

  (let [id (get-id dict-or-id)
        row (fetch [table-name id])]

    (-> (sql/delete table-name id)
        (execute2))

    row))


(defn delete-all
  `Takes a table name, and optional args and deletes the corresponding rows.

  Example:

  (import db)

  (db/delete-all :post :where {:draft true} :limit 1)

  (db/delete-all :post) -> deletes all rows

  (db/delete-all :post :where {:draft true}) -> no limit

  => @[@{:id 1 :title "title" :body "body" :draft true} ...]`
  [table-name & args]
  (let [params (table ;args)
        rows (from table-name ;args)]

    (-> (sql/delete-all table-name params)
        (execute2))

    rows))


(defn write-schema-file []
  (let [rows (query "select sql from sqlite_master where sql is not null order by rootpage")
        schema-sql (as-> rows ?
                         (map |(get $ :sql) ?)
                         (string/join ? "\n"))]
    (file/write-all "db/schema.sql" schema-sql)))


(defn row
  `Executes a query against a postgres database and
   returns the first row.

  Example:

  (import db)

  (db/row "select * from todos where id = ?" 1)

  => @{:id 1 :name "name"}`
  [sql & params]
  (first (query sql params)))


(defn val
  `Executes a query against a postgres database and
   returns the literal value from the select or
   returning statement.

  Example:

  (import db)

  (db/val "select name from todos where id = ?" 1)

  => "todo #1"`
  [sql & params]
  (-> (query sql params)
      (get 0 {})
      (values)
      (first)))


(defn all
  `Executes a query against a postgres database and
   returns the literal value from the select or
   returning statement.

  Example:

  (import db)

  (db/all "select name from todos")

  => @[@{:id 1 :tag-name "tag1"} @{:id 2 :tag-name "tag2"}]`
  [sql & params]
  (query sql params))
