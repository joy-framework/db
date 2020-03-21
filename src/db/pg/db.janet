(import pq)
(import ./sql :as sql)
(import ../helper :prefix "")


(defn connect []
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


(defn query
  `Executes a query against a postgres database.

  Example:

  (import db)

  (db/query "select * from todos")

  # or

  (db/query "select * from todos where id = :id" {:id 1})

  => [{:id 1 :name "name"} {...} ...]`
  [sql &opt params]
  (default params {})
  (let [sql (string sql ";")
        matches (peg/match (capture param-peg) sql)
        pg-params (pg-params matches)
        sql (first (peg/match (replacer param-peg pg-params) sql))
        pq-params (pg-param-args matches params)]
    (pq/all (dyn :db/connection) sql ;pq-params)))


(defn row
  `Executes a query against a postgres database and
   returns the first row.

  Example:

  (import db)

  (db/row "select * from todos where id = :id" {:id 1})

  => {:id 1 :name "name"}`
  [sql &opt params]
  (default params {})
  (let [sql (string sql ";")
        matches (peg/match (capture param-peg) sql)
        pg-params (pg-params matches)
        sql (first (peg/match (replacer param-peg pg-params) sql))
        pq-params (pg-param-args matches params)]
    (pq/row (dyn :db/connection) sql ;pq-params)))


(defn val
  `Executes a query against a postgres database and
   returns the literal value from the select or
   returning statement.

  Example:

  (import db)

  (db/val "select name from todos where id = :id" {:id 1})

  => "name"`
  [sql &opt params]
  (default params {})
  (let [sql (string sql ";")
        matches (peg/match (capture param-peg) sql)
        pg-params (pg-params matches)
        sql (first (peg/match (replacer param-peg pg-params) sql))
        pq-params (pg-param-args matches params)]
    (pq/val (dyn :db/connection) sql ;pq-params)))


(defn execute
  `Executes a query against a postgres database.

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
        matches (peg/match (capture param-peg) sql)
        pg-params (pg-params matches)
        sql (first (peg/match (replacer param-peg pg-params) sql))
        pq-params (pg-param-args matches params)]
    (pq/exec (dyn :db/connection) sql ;pq-params)))


(defn write-schema-file []
  (os/shell (string "pg_dump --schema-only " database-url " > db/schema.sql")))


(defn insert
  `Takes a table name and a dictionary,
  inserts the dictionary as rows/columns into the database
  and returns the inserted row from the database.

  Example:

  (import db)

  (db/insert :todo {:name "name3"})

  => @{:id 3 :name "name3" :completed false}`
  [table-name params]
  (let [sql (sql/insert table-name params)]
    (row sql params)))


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
    (pq/row (dyn :db/connection) sql ;params)))
