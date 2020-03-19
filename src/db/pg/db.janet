(import pq)
(import ./sql :as sql)
(import ../helper :prefix "")


(defn connect []
  (setdyn :db/connection (pq/connect database-url)))


(defn disconnect []
  (pq/close (dyn :db/connection))
  (setdyn :db/connection nil))


(defmacro with-transaction
  `Wrap the current database connection in a transaction`
  [& body]
  ~(pq/tx (,dyn :db/connection) {}
     (try
       ,;body
       (pq/commit (,dyn :db/connection) :success)
       ([err fib]
        (pq/rollback (,dyn :db/connection))
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
        sql (peg/match (replacer param-peg pg-params) sql)
        pq-params (pg-param-args matches params)]
    (pq/all (dyn :db/connection) sql ;pq-params)))


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
        sql (peg/match (replacer param-peg pg-params) sql)
        pq-params (pg-param-args matches params)
        db (dyn :db/connection)]
    (pq/exec db sql params)))


(defn write-schema-file []
  (os/shell (string "pg_dump --schema-only " database-url " > db/schema.sql")))
