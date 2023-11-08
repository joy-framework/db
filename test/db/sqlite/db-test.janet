(import tester :prefix "" :exit true)
(import "src/db/sqlite/db" :as db)
(import "src/db/helper" :prefix "")

(db/connect "test.sqlite3")

(db/execute "drop table if exists post")
(db/execute "drop table if exists author")
(db/execute "drop table if exists tag")
(db/execute "drop table if exists comment")

(db/execute "create table if not exists post (id integer primary key, title text, body text, published_at integer, author_id integer)")
(db/execute "create table if not exists author (id integer primary key, name text)")
(db/execute "create table if not exists tag (id integer primary key, name text, post_id integer)")
(db/execute "create table if not exists comment (id integer primary key, body text, post_id integer)")

# test insert first author
(db/insert :author {:name "author 1"})

(defsuite
  (test "from with null where param"
    (is (deep= @[]
               (db/from :post :where {:title 'null}))))


  (test "insert another table"
    (is (deep= @{:id 1 :name "author 1" :db/table :author}
               (db/find :author 1))))


  (test "insert"
    (is (deep= @{:id 1 :title "title 1" :db/table :post :author-id 1}
               (db/insert :post {:title "title 1" :author-id 1}))))


  (test "upsert"
    (is (deep= @{:id 1 :title "title 2" :db/table :post :author-id 1}
               (db/insert :post
                          {:title "title 1" :author-id 1 :id 1}
                          :on-conflict :id
                          :do :update
                          :set {:title "title 2"}))))


  (test "upsert again"
    (is (deep= @{:id 1 :title "title 1" :db/table :post :author-id 1}
               (db/insert :post
                          {:title "title 1" :author-id 1 :id 1}
                          :on-conflict :id
                          :do :update
                          :set {:title "title 1"}))))


  (test "upsert with nothing"
    (is (deep= nil
               (db/insert :post
                          {:title "title 1" :author-id 1 :id 1}
                          :on-conflict :id
                          :do :nothing))))


  (test "insert-all tag table"
    (is (deep= @[@{:id 1 :name "tag 1" :db/table :tag :post-id 1} @{:id 2 :name "tag 2" :db/table :tag :post-id 1}]
               (db/insert-all :tag [{:name "tag 1" :post-id 1} {:name "tag 2" :post-id 1}]))))


  (test "insert-all comment table"
    (is (deep= @[@{:id 1 :body "comment 1" :db/table :comment :post-id 1} @{:id 2 :body "comment 2" :db/table :comment :post-id 1}]
               (db/insert-all :comment [{:body "comment 1" :post-id 1} {:body "comment 2" :post-id 1}]))))


  (test "insert with multiple params"
     (is (deep= @{:id 2 :title "title 2" :body "body 2" :db/table :post}
                (db/insert :post {:title "title 2" :body "body 2"}))))


  (test "insert with single argument"
    (is (deep= @{:id 3 :title "title 3" :body "body 3" :db/table :post}
               (db/insert {:db/table :post :title "title 3" :body "body 3"}))))

  (test "query"
    (is (deep= @[@{:id 2 :title "title 2" :body "body 2"}]
               (db/query "select * from post where body = :body" {:body "body 2"}))))


  (test "row"
    (is (deep= @{:id 2 :title "title 2" :body "body 2"}
               (db/row "select * from post where body = ?" "body 2"))))


  (test "val"
    (is (deep= "title 1"
               (db/val "select title from post where id = ?" 1))))

  (test "all"
    (is (deep= @[@{:id 2 :title "title 2" :body "body 2"}]
               (db/all "select * from post where body = ?" "body 2"))))


  (test "fetch"
    (is (deep= @{:id 1 :title "title 1" :db/table :post :author-id 1}
               (db/fetch [:post 1]))))


  (test "fetch-all"
    (is (deep= @[@{:id 1 :title "title 1" :db/table :post :author-id 1}
                 @{:id 2 :title "title 2" :body "body 2" :db/table :post}
                 @{:id 3 :title "title 3" :body "body 3" :db/table :post}]
               (db/fetch-all [:post] :order "id"))))


  (test "fetch with join"
    (is (deep= @{:id 1 :title "title 1" :db/table :post :author-id 1}
               (db/fetch [:author 1 :post 1]))))


  (test "fetch multiple rows with join"
    (is (deep= @[@{:id 1 :title "title 1" :db/table :post :author-id 1}]
               (db/fetch-all [:author 1 :post]))))


  (test "from"
    (is (deep= @[@{:id 1 :title "title 1" :db/table :post :author-id 1}]
               (db/from :post :where {:title "title 1"} :limit 1))))


  (test "from with tuple"
    (is (deep= @[1 2 3]
               (map |(get $ :id) (db/from :post :where {:id [1 2 3]})))))


  (test "join"
    (is (deep= @[@{:id 1 :title "title 1" :db/table :post :author-id 1 :author/id 1 :author/name "author 1"}]
               (db/from :post :join :author :limit 1 :order "id"))))


  (test "join with multiple join tables"
    (is (deep= @[@{:db/table :post
                   :title "title 1"
                   :author/name "author 1"
                   :tag/post-id 1
                   :tag/name "tag 1"
                   :author-id 1
                   :id 1
                   :author/id 1
                   :tag/id 1}
                 @{:db/table :post
                   :title "title 1"
                   :author/name "author 1"
                   :tag/post-id 1
                   :tag/name "tag 2"
                   :author-id 1
                   :id 1
                   :author/id 1
                   :tag/id 2}]
               (db/from :post :join [:author :tag]))))


  (test "join/one"
    (is (deep= @[@{:id 1 :title "title 1" :db/table :post :author-id 1 :author @{:id 1 :db/table :author :name "author 1"}}]
               (db/from :post :join/one :author :limit 1 :order "id"))))


  (test "join/many"
    (is (deep= @[@{:id 1
                   :title "title 1"
                   :db/table :post
                   :author-id 1
                   :tags @[@{:id 1 :db/table :tag :name "tag 1" :post-id 1}
                           @{:id 2 :db/table :tag :name "tag 2" :post-id 1}]}]
               (db/from :post :join/many :tag :where {:post.id 1}))))


  (test "join/many with multiple join tables"
    (is (deep= @[@{:id 1
                   :title "title 1"
                   :db/table :post
                   :author-id 1
                   :tags @[@{:id 1 :db/table :tag :name "tag 1" :post-id 1}
                           @{:id 2 :db/table :tag :name "tag 2" :post-id 1}]
                   :comments @[@{:id 1 :db/table :comment :body "comment 1" :post-id 1}
                               @{:id 2 :db/table :comment :body "comment 2" :post-id 1}]}]
               (db/from :post
                        :join/many [:tag :comment]
                        :where {:post.id 1}))))


  (test "find-by"
    (is (deep= @{:id 2 :title "title 2" :body "body 2" :db/table :post}
               (db/find-by :post :where {:body "body 2"}))))


  (test "find"
    (is (deep= @{:id 2 :title "title 2" :body "body 2" :db/table :post}
               (db/find :post 2))))


  (test "insert-all"
    (is (deep= @[@{:id 4 :title "title 4" :db/table :post} @{:id 5 :title "title 5" :db/table :post}]
               (db/insert-all :post [{:title "title 4"} {:title "title 5"}]))))


  (test "insert-all with multiple params"
    (is (deep= @[@{:id 6 :title "title 6" :body "body 6" :db/table :post} @{:id 7 :title "title 7" :body "body 7" :db/table :post}]
               (db/insert-all :post [{:title "title 6" :body "body 6"} {:title "title 7" :body "body 7"}]))))


  (test "update"
    (is (deep= @{:id 4 :title "title4" :body "body4" :db/table :post}
               (db/update :post 4 {:title "title4" :body "body4"}))))


  (test "update with one dictionary"
    (is (deep= @{:id 4 :title "title4" :body "body4" :db/table :post}
               (db/update {:db/table :post :id 4 :title "title4" :body "body4"}))))


  (test "update with two dictionaries"
    (is (deep= @{:id 4 :title "title4" :body "body4" :db/table :post}
               (db/update {:db/table :post :id 4} {:title "title4" :body "body4"}))))


  (test "update-all"
    (is (deep= @[@{:id 4 :title "title 4" :body "body 4" :db/table :post}]
               (db/update-all :post :set {:title "title 4" :body "body 4"} :where {:title "title4"}))))


  (test "update set to null"
    (is (deep= @{:id 6 :body "body 6" :db/table :post}
               (db/update :post 6 {:title :null}))))


  (test "another from with null in where"
    (is (deep= @[@{:id 6 :body "body 6" :db/table :post}]
               (db/from :post :where {:title :null}))))


  (test "update-all with null in where"
    (is (deep= @[@{:id 6 :title "title 6" :body "body 6" :db/table :post}]
               (db/update-all :post :set {:title "title 6"} :where {:title 'null}))))


  (test "delete one"
    (is (deep= @{:id 6 :title "title 6" :body "body 6" :db/table :post}
               (db/delete :post 6))))

  (test "insert new row"
    (is (deep= @{:id 8 :title "title 2" :db/table :post :author-id 2}
               (db/insert :post {:title "title 2" :author-id 2}))))

  (test "insert new row"
    (is (deep= @{:id 9 :title "title 2" :db/table :post :author-id 2}
               (db/insert :post {:title "title 2" :author-id 2}))))

  (test "upsert second to last row inserted, expecting to get back the relevant row updated"
    (is (deep= @{:id 8 :title "title 1" :db/table :post :author-id 2}
               (db/insert :post
                          {:title "title 1" :author-id 2 :id 8}
                          :on-conflict :id
                          :do :update
                          :set {:title "title 1"}))))

  (test "delete all"
    (is (deep= @[] (do
                     (db/delete-all :post)
                     (db/from :post))))))
