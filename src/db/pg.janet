(import pq)
(import ./pg/sql :as sql)
(import ./helper :prefix "")


(def database-url (os/getenv "DATABASE_URL"))


(defn connect []
  (setdyn :db/connection (pq/connect database-url)))


(defn disconnect []
  (pq/close (dyn :db/connection))
  (setdyn :db/connection nil))


(defmacro with-transaction
  `Wrap the current database connection in a transaction`
  [& body]
  ~(pq/tx (dyn :db/connection) {}
     (try
       ,;body
       (pq/commit (dyn :db/connection) :success)
      ([err fib]
       (pq/rollback (dyn :db/connection))
       (propagate err fib)))))



(defn query
  `Executes a query against a postgres database.

  Example:

  (import db)

  (db/query "select * from todos")

  # or

  (db/query "select * from todos where id = ?" 1)

  => [{:id 1 :name "name"} {...} ...]`
  [sql & params]
  (let [sql (string sql ";")
        db (dyn :db/connection)
        pq-params @[]]
    (var i 0)
    (loop [p :in params]
      (++ i)
      (array/push pq-params (string "$" i)))

    (pq/all db sql ;pq-params)))


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
  (let [sql (string sql ";")
        params (snake-case-keys params)
        db (dyn :db/connection)]
    (pq/exec db sql params)
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
  (let [params {:rowid rowid}
        sql (sql/from table-name {:where params :limit 1})]
    (as-> (query sql params) ?
          (get ? 0 {}))))


(def schema-sql
  `select
    m.name as tbl,
    pti.name as col
  from sqlite_master m
  join pragma_table_info(m.name) pti on m.name != pti.name`)

(defn schema []
  (as-> schema-sql ?
        (query ?)
        (filter |(= "updated_at" (get $ :col)) ?)
        (map |(table (get $ :tbl) (get $ :col)) ?)
        (apply merge ?)))


(defn fetch
  `Takes a path into the db and optional args
   and returns the first row that matches or nil if none exists.

  Example:

  (import db)

  (db/fetch [:todo 1])

  => {:id 1 :name "name"}`
  [path & args]
  (let [args (table ;args)
        sql (sql/fetch path (merge args {:limit 1}))
        params (sql/fetch-params path)]
    (as-> (query sql params) ?
          (get ? 0))))


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
  (let [sql (sql/fetch path (table ;args))
        params (sql/fetch-params path)]
    (query sql params)))


(defn from
  `Takes an optional db connection, a table name and optional args
   and returns all of the rows that match the query
   or an empty array if no rows match.

  Example:

  (import db)

  (db/from :todo :where {:completed true} :order "name" :limit 2)

  # or

  (db/from :todo :where {:completed true} :order "name desc" :limit 10)

  => [{:id 1 name "name" :completed true} {:id 1 :name "name2" :completed true}]`
  [table-name & args]
  (let [opts (table ;args)
        sql (sql/from table-name opts)
        params (get opts :where {})
        params (as-> params ?
                     (pairs ?)
                     (filter (fn [[k v]] (not= 'null v)) ?)
                     (mapcat identity ?)
                     (apply table ?))]
    (query sql params)))


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
  (let [opts (table ;args)
        sql (sql/from table-name opts)
        params (get opts :where {})
        rows (query sql params)]
    (get rows 0)))


(defn find
  `Takes a table name and optional args
   and returns either nil or the first row by primary key.

  Example:

  (import db)

  (db/find :todo 1)

  => {:id 1 name "name" :completed true}`
  [table-name id]
  (let [sql (sql/from table-name {:where {:id id} :limit 1})
        rows (query sql {:id id})]
    (get rows 0)))


(defn insert
  `Takes an optional db connection, a table name and a dictionary,
  inserts the dictionary as rows/columns into the database
  and returns the inserted row from the database.

  Example:

  (import db)

  (db/insert :todo {:name "name3"})

  => {:id 3 :name "name3" :completed false}`
  [table-name params]
  (let [sql (sql/insert table-name params)]
    (as-> (execute sql params) ?
          (last-inserted table-name ?))))


(defn insert-all
  `Takes an optional db connection, a table name and an array of dictionaries,
   inserts the array into the database and returns the inserted rows.
   All keys must be the same, as it only insert into one table at a time.

  Example:

  (import db)

  (db/insert-all :todo [{:name "name4"} {:name "name5"}])

  => [{:id 4 :name "name4" :completed false} {:id 5 :name "name5" :completed false}]`
  [table-name arr]
  (let [sql (sql/insert-all table-name arr)
        params (sql/insert-all-params arr)]
    (execute sql params)
    (query (string "select * from " (snake-case table-name) " order by rowid limit " (length params)))))


(defn get-id [val]
  (if (dictionary? val)
    (get val :id)
    val))


(defn update
  `Takes an optional db connection, a table name and a dictionary with an :id key OR an id value,
  and a dictionary with the new columns/values to be updated, updates the row in the
  database and returns the updated row.

  Example:

  (import db)

  (db/update :todo 4 {:name "new name 4"})

  (db/update :todo {:id 4} {:name "new name 4"})

  => {:id 4 :name "new name 4" :completed false}`
  [table-name dict-or-id params]
  (let [sql-table-name (snake-case table-name)
        schema (schema)
        params (if (and (dictionary? schema)
                        (= "updated_at" (get schema sql-table-name)))
                 (merge params {:updated-at (os/time)})
                 params)
        sql (sql/update table-name params)
        id (get-id dict-or-id)]
    (execute sql (merge params {:id id}))
    (fetch [table-name id])))


(defn update-all
  `Takes a table name a dictionary representing the where clause
   and a dictionary representing the set clause and updates the rows in the
   database and returns them.

  Example:

  (import db)

  (db/update-all :todo {:completed false} {:completed true})

  => [{:id 1 :completed true} ...]`
  [table-name where-params set-params]
  (let [rows (from table-name :where where-params)
        sql (sql/update-all table-name where-params set-params)
        schema (schema)
        set-params (if (and (dictionary? schema)
                            (= "updated_at" (get schema (snake-case table-name))))
                     (merge set-params {:updated-at (os/time)})
                     set-params)
        params (sql/update-all-params where-params set-params)]
    (execute sql params)
    (from table-name :where (as-> rows ?
                                  (map |(table :id (get $ :id)) ?)
                                  (apply merge ?)))))


(defn delete
  `Takes a table name, a dictionary with an :id key or an id value
   representing the primary key integer row in the database, executes a DELETE and
   returns the deleted row.

  Example:

  (import db)

  (db/delete :todo {:id 1})

  (db/delete :todo 1)

  => {:id 1 :name "name" :completed true}`
  [table-name dict-or-id]
  (let [id (get-id dict-or-id)
        row (fetch [table-name id])
        sql (sql/delete table-name id)
        params {:id id}]
    (execute sql params)
    row))


(defn delete-all
  `Takes a db connection, a table name, and optional args and deletes the corresponding rows.

  Example:

  (import db)

  (db/delete-all :post :where {:draft true} :limit 1)

  (db/delete-all :post) -> deletes all rows

  (db/delete-all :post :where {:draft true}) -> no limit

  => [{:id 1 :title "title" :body "body" :draft true} ...]`
  [table-name & args]
  (let [params (table ;args)
        where-params (get params :where {})
        rows (from table-name ;args)
        sql (sql/delete-all table-name params)]
    (execute sql where-params)
    rows))
