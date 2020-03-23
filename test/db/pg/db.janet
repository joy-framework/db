(import tester :prefix "" :exit true)

# if you're running this test be sure to run
# db new datbase:postgres db
# from your terminal
(os/setenv "DATABASE_URL" "postgres://localhost/db")
(import "src/db/pg/db" :as db)
(db/connect)

(db/execute "drop table account")
(db/execute "create table account (id serial primary key, name text, code text, code_expires_at integer)")

(deftest
  (test "insert"
    (deep= @{:id 1 :name "name"}
           (db/insert :account {:name "name"})))

  (test "insert with multiple params"
    (deep= @{:id 2 :name "name" :code "code"}
           (db/insert :account {:name "name" :code "code"})))

  (test "query"
    (deep= @{:id 2 :name "name" :code "code"}
           (first (db/query "select * from account where code = :code" {:code "code"}))))

  (test "row"
    (deep= @{:id 2 :name "name" :code "code"}
           (db/row "select * from account where code = $1" "code")))

  (test "val"
    (deep= "name"
           (db/val "select name from account where code = $1" "code")))

  (test "all"
    (deep= @[@{:id 2 :name "name" :code "code"}]
           (db/all "select * from account where code = $1" "code")))

  (test "fetch"
    (deep= @{:id 1 :name "name"}
           (db/fetch [:account 1])))

  (test "fetch-all"
    (deep= @[@{:id 1 :name "name"} @{:id 2 :name "name" :code "code"}]
           (db/fetch-all [:account] :order "id")))

  (test "from"
    (deep= @[@{:id 2 :name "name" :code "code"}]
           (db/from :account :where {:code "code"} :order "id")))

  (test "find-by"
    (deep= @{:id 2 :name "name" :code "code"}
           (db/find-by :account :where {:code "code"})))

  (test "find"
    (deep= @{:id 2 :name "name" :code "code"}
           (db/find :account 2))))


(db/disconnect)
