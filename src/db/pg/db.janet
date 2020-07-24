(import pq)
(import ./sql :as sql)
(import ../helper :prefix "")


(defn connect [&opt url]
  (default url database-url)

  (unless url
    (error "DATABASE_URL environment variable isn't set"))

  (setdyn :db/connection (pq/connect database-url)))


(defn disconnect []
  (pq/close (dyn :db/connection))
  (setdyn :db/connection nil))


(defmacro with-connection
  [& body]
  ~(do
     (,connect)
     ,;body
     (,disconnect)))


(defmacro with-transaction
  `Wrap the current database connection in a transaction`
  [& body]
  ~(,pq/txn (,dyn :db/connection) {}
     (try
       (do
         ,;body)
       ([err fib]
        (printf "%q" err)
        (,pq/rollback (,dyn :db/connection))
        (propagate err fib)))))


(defn- pg-params [matches]
  (let [acc @{}]
    (var i 0)
    (loop [m :in matches]
      (put acc m (string "$" (++ i))))
    acc))


(defn- pg-param-args [matches params]
  (let [acc @[]]
    (var i 0)
    (loop [m :in matches]
      (array/push acc (get params (keyword (drop 1 m)))))
    acc))


(def- param-peg '(<- (sequence ":" (some (choice (range "az" "AZ" "09") (set "-_"))))))


(defn- capture [str]
  (peg/compile ~(any (+ (* ,str) 1))))


(defn- replacer [patt subst]
  (peg/compile ~(% (any (+ (/ (<- ,patt) ,subst) (<- 1))))))


(defn- pq-sql [sql params]
  (let [matches (peg/match (capture param-peg) sql)
        pg-params (pg-params matches)]
    (first (peg/match (replacer param-peg pg-params) sql))))


(defn- pq-params [sql params]
  (let [matches (peg/match (capture param-peg) sql)]
    (pg-param-args matches params)))


(defn query
  `Executes a query against a postgres database.

  Example:

  (import db)

  (db/query "select * from todos")

  # or

  (db/query "select * from todos where id = :id" {:id 1})

  # or

  (db/query "select * from todos where id = $1" [1])

  => [{:id 1 :name "name"} {...} ...]`
  [sql &opt params]
  (default params {})
  (let [sql (string sql ";")
        [sql params] (if (dictionary? params)
                       [(pq-sql sql params) (pq-params sql params)]
                       [sql params])]
    (pq/all (dyn :db/connection) sql ;params)))


(defn execute
  `Executes a query against a postgres database.

  The first arg is the sql to execute, the second optional arg is a dictionary
  for any values you want to pass in.

  Example:

  (import db)

  (db/execute "create table todo (id integer primary key, name text)")

  # or

  (db/execute "insert into todo (id, name) values (:id, :name)" {:id 1 :name "name"})

  # or

  (db/execute "insert into todo (id, name) values ($1, $2)" [1 "name"])`
  [sql &opt params]
  (default params {})
  (let [sql (string sql ";")
        [sql params] (if (dictionary? params)
                       [(pq-sql sql params) (pq-params sql params)]
                       [sql params])]
    (pq/exec (dyn :db/connection) sql ;params)))


(defn write-schema-file []
  (os/shell (string "pg_dump --schema-only " database-url " > db/schema.sql")))


(defn row
  `Executes a query against a postgres database and
   returns the first row.

  Example:

  (import db)

  (db/row "select * from todos where id = $1" 1)

  => {:id 1 :name "name"}`
  [sql & params]
  (let [sql (string sql ";")]
    (pq/row (dyn :db/connection) sql ;params)))


(defn val
  `Executes a query against a postgres database and
   returns the literal value from the select or
   returning statement.

  Example:

  (import db)

  (db/val "select name from todos where id = $1" 1)

  => "todo #1"`
  [sql & params]
  (let [sql (string sql ";")]
    (pq/val (dyn :db/connection) sql ;params)))


(defn all
  `Executes a query against a postgres database and
   returns the literal value from the select or
   returning statement.

  Example:

  (import db)

  (db/all "select name from todos" 1)

  => @[@{:id 1 :tag-name "tag1"} {:id 2 :tag-name "tag2"}]`
  [sql & params]
  (let [sql (string sql ";")]
    (pq/all (dyn :db/connection) sql ;params)))


(def schema-sql
  `select
    t.table_name as tbl,
    c.column_name as col
  from information_schema.tables t
  join information_schema.columns c on c.table_name = t.table_name`)

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

  => @{:id 1 :name "name"}`
  [path & args]
  (let [args (table ;args)
        sql (sql/fetch path (merge args {:limit 1}))
        params (sql/fetch-params path)]
    (row sql ;params)))


(defn fetch-all
  `Takes a path into the db and optional args
   and returns all of the rows that match or an empty array if
   no rows match.

  Example:

  (import db)

  (db/fetch-all [:todo 1 :tag] :order "tag_name asc")

  (db/fetch-all [:todo 1] :limit 1 :order "tag_name desc")

  => @[@{:id 1 :tag-name "tag1"} @{:id 2 :tag-name "tag2"}]`
  [path & args]
  (let [sql (sql/fetch path (table ;args))
        params (sql/fetch-params path)]
    (all sql ;params)))


(defn from
  `Takes a table name and optional args
   and returns all of the rows that match the query
   or an empty array if no rows match.

  Example:

  (import db)

  (db/from :todo :where {:completed true} :order "name" :limit 2)

  # or

  (db/from :todo :where {:completed true} :order "name desc" :limit 10)

  => @[@{:id 1 name "name" :completed true} @{:id 1 :name "name2" :completed true}]`
  [table-name & args]
  (let [opts (table ;args)
        sql (sql/from table-name opts)
        params (get opts :where {})
        params (as-> params ?
                     (values ?)
                     (filter |(not (sql/null? $)) ?))]
    (all sql ;params)))


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
        params (as-> params ?
                     (values ?)
                     (filter |(not (sql/null? $)) ?))]
    (row sql ;params)))


(defn find
  `Takes a table name and optional args
   and returns either nil or the first row by primary key.

  Example:

  (import db)

  (db/find :todo 1)

  => {:id 1 name "name" :completed true}`
  [table-name id]
  (let [sql (sql/from table-name {:where {:id id} :limit 1})]
    (row sql id)))


(defn insert
  `Takes a table name and a dictionary,
  inserts the dictionary as rows/columns into the database
  and returns the inserted row from the database.

  Example:

  (import db)

  (db/insert :todo {:name "name3"})

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

  (let [sql (sql/insert table-name params)]
    (row (pq-sql sql params) ;(pq-params sql params))))


(defn insert-all
  `Takes a table name and an array of dictionaries,
   inserts the array into the database and returns the inserted rows.

   All keys must be the same.

   Example:

   (import db)

   (db/insert-all :todo [{:name "name4"} {:name "name5"}])

   => @[@{:id 4 :name "name4" :completed false} @{:id 5 :name "name5" :completed false}]`
  [table-name arr]
  (let [sql (sql/insert-all table-name arr)
        params (mapcat values arr)]
    (all sql ;params)))


(defn get-id [val]
  (if (dictionary? val)
    (get val :id)
    val))


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
    (do
      (set table-name (get (args 0) :db/table))
      (set dict-or-id (args 0))
      (set params (put (table ;(kvs (args 0))) :db/table nil))))

  (let [sql-table-name (snake-case table-name)
        schema (schema)
        params (if (and (dictionary? schema)
                        (find-index |(= $ :updated_at) (get schema (keyword (snake-case table-name)))))
                 (merge params {:updated-at (os/time)})
                 params)
        sql (sql/update sql-table-name params)
        id (get-id dict-or-id)]
    (first (query sql (merge params {:id id})))))


(defn update-all
  `Takes a table name a dictionary representing the where clause
   and a dictionary representing the set clause and updates the rows in the
   database and returns them.

  Example:

  (import db)

  (db/update-all :todo {:completed false} {:completed true})

  => @[@{:id 1 :completed true} ...]`
  [table-name where-params set-params]
  (let [rows (from table-name :where where-params)
        sql (sql/update-all table-name where-params set-params)
        schema (schema)
        set-params (if (and (dictionary? schema)
                            (find-index |(= $ :updated_at) (get schema (keyword (snake-case table-name)))))
                     (merge set-params {:updated-at (os/time)})
                     set-params)
        params (sql/update-all-params where-params set-params)]
    (query sql params)))


(defn delete
  `Takes a table name, a dictionary with an :id key or an id value
   representing the primary key integer row in the database, executes a DELETE and
   returns the deleted row.

  Example:

  (import db)

  (db/delete :todo {:id 1})

  (db/delete :todo 1)

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
        sql (sql/delete table-name)]
    (row sql id)))


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
        where-params (get params :where {})
        rows (from table-name ;args)
        sql (sql/delete-all table-name params)]
    (execute sql where-params)
    rows))
