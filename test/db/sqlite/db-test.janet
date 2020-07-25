(import tester :prefix "" :exit true)
(import "src/db/sqlite/db" :as db)

(db/connect "test.sqlite3")

(db/execute "drop table if exists post")
(db/execute "create table if not exists post (id integer primary key, title text, body text, published_at integer)")

(deftest
  (test "insert"
    (deep= @{:id 1 :title "the title" :db/table :post}
           (db/insert :post {:title "the title"})))

  (test "insert with multiple params"
    (deep= @{:id 2 :title "the title" :body "this is the body" :db/table :post}
           (db/insert :post {:title "the title" :body "this is the body"})))

  (test "insert with single argument"
    (do
      (db/delete :post 2)
      (deep= @{:id 2 :title "the title" :body "this is the body" :db/table :post}
             (db/insert {:db/table :post :title "the title" :body "this is the body"}))))

  (test "query"
    (deep= @{:id 2 :title "the title" :body "this is the body"}
           (first (db/query "select * from post where body = :body" {:body "this is the body"}))))

  (test "row"
    (deep= @{:id 2 :title "the title" :body "this is the body"}
           (db/row "select * from post where body = ?" "this is the body")))

  (test "val"
    (deep= "the title"
           (db/val "select title from post where body = ?" "this is the body")))

  (test "all"
    (deep= @[@{:id 2 :title "the title" :body "this is the body"}]
           (db/all "select * from post where body = ?" "this is the body")))

  (test "fetch"
    (deep= @{:id 1 :title "the title" :db/table :post}
           (db/fetch [:post 1])))

  (test "fetch-all"
    (deep= @[@{:id 1 :title "the title" :db/table :post} @{:id 2 :title "the title" :body "this is the body" :db/table :post}]
           (db/fetch-all [:post] :order "id")))

  (test "from"
    (deep= @[@{:id 2 :title "the title" :body "this is the body" :db/table :post}]
           (db/from :post :where {:body "this is the body"} :order "id")))

  (test "find-by"
    (deep= @{:id 2 :title "the title" :body "this is the body" :db/table :post}
           (db/find-by :post :where {:body "this is the body"})))

  (test "find"
    (deep= @{:id 2 :title "the title" :body "this is the body" :db/table :post}
           (db/find :post 2)))

  (test "insert-all"
    (deep= @[@{:id 3 :title "name3" :db/table :post} @{:id 4 :title "name4" :db/table :post}]
           (db/insert-all :post [{:title "name3"} {:title "name4"}])))

  (test "insert-all with multiple params"
    (deep= @[@{:id 5 :title "name5" :body "code2" :db/table :post} @{:id 6 :title "name6" :body "code1" :db/table :post}]
           (db/insert-all :post [{:title "name5" :body "code2"} {:title "name6" :body "code1"}])))

  (test "delete one"
    (deep= @{:id 6 :title "name6" :body "code1" :db/table :post}
           (db/delete :post 6)))

  (test "delete all"
    (empty? (do
              (db/delete-all :post)
              (db/fetch-all [:post])))))

(db/disconnect)
