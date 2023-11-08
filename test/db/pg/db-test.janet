(import tester :prefix "" :exit true)

(if (string/has-prefix? "postgres" (or (os/getenv "DATABASE_URL") ""))
  (import /src/db/pg/db :as db)
  (import /src/db/sqlite/db :as db))


(when (string/has-prefix? "postgres" (or (os/getenv "DATABASE_URL") ""))
  (os/shell "dropdb test_db")
  (os/shell "db new database:postgres test_db")

  (db/connect)

  (db/execute "drop table if exists account")
  (db/execute "create table if not exists account (id serial primary key, name text, code text, code_expires_at integer)")

  (deftest
    (test "insert"
      (deep= @{:id 1 :name "name" :db/table :account}
             (db/insert :account {:name "name"})))

    (test "insert with multiple params"
      (deep= @{:id 2 :name "name" :code "code" :db/table :account}
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
      (deep= @{:id 1 :name "name" :db/table :account}
             (db/fetch [:account 1])))

    (test "fetch-all"
      (deep= @[@{:id 1 :name "name" :db/table :account} @{:id 2 :name "name" :code "code" :db/table :account}]
             (db/fetch-all [:account] :order "id")))

    (test "from"
      (deep= @[@{:id 2 :name "name" :code "code" :db/table :account}]
             (db/from :account :where {:code "code"} :order "id")))

    (test "find-by"
      (deep= @{:id 2 :name "name" :code "code" :db/table :account}
             (db/find-by :account :where {:code "code"})))

    (test "find"
      (deep= @{:id 2 :name "name" :code "code" :db/table :account}
             (db/find :account 2)))

    (test "insert-all"
      (deep= @[@{:id 3 :name "name3" :db/table :account} @{:id 4 :name "name4" :db/table :account}]
             (db/insert-all :account [{:name "name3"} {:name "name4"}])))

    (test "insert-all with multiple params"
      (deep= @[@{:id 5 :name "name5" :code "code2" :db/table :account} @{:id 6 :name "name6" :code "code1" :db/table :account}]
             (db/insert-all :account [{:name "name5" :code "code2"} {:name "name6" :code "code1"}])))

    (test "update"
      (deep= @{:id 5 :code "code2" :name "name7" :db/table :account}
             (db/update :account 5 {:name "name7"})))

    (test "update-all"
      (deep= @[@{:name "name" :code "updated code" :id 2 :db/table :account}]
             (db/update-all :account {:code "code"} {:code "updated code"})))

    (test "delete one"
      (deep= @{:id 6 :name "name6" :code "code1" :db/table :account}
             (db/delete :account 6)))

    (test "delete all"
      (empty? (do
                (db/delete-all :account)
                (db/fetch-all [:account])))))


  (db/disconnect))
