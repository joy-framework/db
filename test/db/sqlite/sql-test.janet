(import tester :prefix "" :exit true)
(import /src/db/sqlite/sql :as sql)

(deftest
  (test "from"
    (is (deep= ["select * from users"]
               (sql/from "users" [] {}))))

  (test "from with limit, offset and order by"
    (is (= ["select * from users order by created_at desc limit 10 offset 10"]
           (sql/from "users" {:order "created_at desc" :limit 10 :offset 10} {}))))

  (test "from with where"
    (is (= ["select * from users where id = ?" 1]
           (sql/from "users" {:where {:id 1}} {}))))


  (test "from with fk join"
    (is (= ["select posts.*, authors.id as 'authors/id', authors.name as 'authors/name' from posts join authors on authors.id = posts.authors_id"]
           (sql/from "posts" {:join :authors} {"authors" ["authors.id" "authors.name"] "posts" ["posts.authors_id"]}))))


  (test "from with many join"
    (is (= ["select posts.*, tags.id as 'tags/id', tags.name as 'tags/name' from posts join tags on tags.posts_id = posts.id"]
           (sql/from "posts" {:join :tags} {"tags" ["tags.id" "tags.name"]}))))


  (test "insert"
    (is (= ["insert into account (name) values (?)" "joy"]
           (sql/insert :account {:name "joy"}))))


  (test "insert with multiple params"
    (is (= ["insert into account (password, name) values (?, ?)" "secret" "joy"]
           (sql/insert :account {:name "joy" :password "secret"}))))


  (test "insert with null param"
    (is (= ["insert into account (name) values (?)" nil]
           (sql/insert :account {:name 'null}))))


  (test "insert with multiple params with dashes"
    (is (= ["insert into account (password, name, created_at) values (?, ?, ?)"
            "secret" "joy" "created-at"]
           (sql/insert :account {:name "joy" :password "secret" :created-at "created-at"}))))


  (test "insert-all test"
    (is (= ["insert into account (name) values (?), (?)" "joy" "ful"]
           (sql/insert-all :account [{:name "joy"} {:name "ful"}]))))


  (test "insert-all test with two params"
    (is (= ["insert into account (email, name) values (?, ?), (?, ?)" "sean@example.com" "sean" "swlkr@example.com" "swlkr"]
           (sql/insert-all :account [{:name "sean" :email "sean@example.com"} {:name "swlkr" :email "swlkr@example.com"}]))))


  (test "insert-all test with kebab-cased params"
    (is (= ["insert into table_a_b_c (email, a_b_c, name) values (?, ?, ?), (?, ?, ?)"
            "email" "" "name1"
            "email2" "" "name2"]
           (sql/insert-all :table-a-b-c [{:name "name1" :email "email" :a-b-c ""} {:name "name2" :email "email2" :a-b-c ""}]))))


  (test "update with dictionary where clause"
    (is (= ["update account set name = ? where id = ?" "name" 1]
           (sql/update :account {:name "name"} {:id 1}))))


  (test "update with null value"
    (is (= ["update account set name = ? where id = ?" nil 1]
           (sql/update :account {:name 'null} {:id 1}))))


  (test "update with null value, empty string and kebab-case params"
    (is (= ["update account set a_b_c = ?, name = ? where id = ?" "" nil 1]
           (sql/update :account {:name 'null :a-b-c ""} {:id 1}))))


  (test "update-all test with same where and set keys"
    (is (= ["update account set name = ? where name = ?" "new name" "old name"]
           (sql/update-all :account {:name "new name"} {:name "old name"}))))


  (test "delete test"
    (is (= ["delete from account where id = ?" 1]
           (sql/delete :account 1))))


  (test "delete-all test with params dictionary"
    (is (= ["delete from account where name = ?" "name"]
           (sql/delete-all :account {:where {:name "name"}}))))


  (test "delete-all test with params dictionary kebab case"
    (is (= ["delete from account where a_b_c = ?" ""]
           (sql/delete-all :account {:where {:a-b-c ""}}))))


  (test "delete-all test with params string"
    (is (= ["delete from account where name = ? or name is null" "name"]
           (sql/delete-all :account {:where ["name = ? or name is null" "name"]}))))


  (test "from test"
    (is (= ["select * from account where name = ?" "name"]
           (sql/from :account {:where {:name "name"}}))))

  (test "from with options test"
    (is (= ["select * from account where name = ? order by rowid desc limit 3" "name"]
           (sql/from :account {:where {:name "name"} :order "rowid desc" :limit 3}))))


  (test "from with join many (has-many) test"
    (is (= ["select accounts.*, posts.id as 'posts/id', posts.name as 'posts/name' from accounts join posts on posts.accounts_id = accounts.id where name = ? order by rowid desc limit 3" "name"]
           (sql/from :accounts
                     {:join :posts :where {:name "name"} :order "rowid desc" :limit 3}
                     {"accounts" ["accounts.id" "accounts.name"]
                      "posts" ["posts.id" "posts.name"]}))))


  (test "from with join one (belongs-to) test"
    (is (= ["select post.*, account.id as 'account/id', account.name as 'account/name' from post join account on account.id = post.account_id where name = ? order by rowid desc limit 3"
            "name"]
           (sql/from :post
                     {:join :account :where {:name "name"} :order "rowid desc" :limit 3}
                     {"post" ["post.id" "post.name" "post.account_id"]
                      "account" ["account.id" "account.name"]}))))


  (test "multiple joins to same table"
    (is (= ["select post.*, account.id as 'account/id', account.name as 'account/name', category.id as 'category/id', category.name as 'category/name' from post join account on account.id = post.account_id join category on category.post_id = post.id where name = ? order by rowid desc limit 3" "name"]
           (sql/from :post
                     {:join [:account :category] :where {:name "name"} :order "rowid desc" :limit 3}
                     {"post" ["post.account_id"]
                      "account" ["account.id" "account.name"]
                      "category" ["category.id" "category.name"]}))))


  (test "fetch test with one table no ids"
    (is (= ["select account.* from account"]
           (sql/fetch [:account]))))


  (test "fetch test with one table and one id"
    (is (= ["select account.* from account where account.id = ?" 1]
           (sql/fetch [:account 1]))))


  (test "fetch test with two tables and one id"
    (is (= ["select todo.* from todo join account on account.id = todo.account_id where account.id = ?" 1]
           (sql/fetch [:account 1 :todo]))))


  (test "fetch test with two tables and two ids"
    (is (= ["select todo.* from todo join account on account.id = todo.account_id where account.id = ? and todo.id = ?" 1 2]
           (sql/fetch [:account 1 :todo 2]))))


  (test "fetch test with two tables and two ids"
    (is (= ["select todo.* from todo join account on account.id = todo.account_id where account.id = ? and todo.id = ?" 1 2]
           (sql/fetch [:account 1 :todo 2]))))


  (test "fetch test with three tables and two ids"
    (is (= ["select comment.* from comment join todo on todo.id = comment.todo_id join account on account.id = todo.account_id where account.id = ? and todo.id = ?" 1 2]
           (sql/fetch [:account 1 :todo 2 :comment]))))


  (test "fetch test with one table and options"
    (is (= ["select account.* from account limit 10"]
           (sql/fetch [:account] {:limit 10}))))


  (test "fetch test with one table and limit and offset options"
    (is (= ["select account.* from account limit 10 offset 2"]
           (sql/fetch [:account] {:limit 10 :offset 2}))))


  (test "where with a tuple"
    (is (= ["select * from account where id = ?" 1]
           (sql/from :account {:where ["id = ?" 1]} {}))))


  (test "from with a tuple"
    (is (= ["select * from users where code = ?" 123]
           (sql/from :users {:where ["code = ?" 123]} {})))))
