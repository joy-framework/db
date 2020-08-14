(import tester :prefix "" :exit true)
(import "src/db/sqlite/sql" :as sql)

(deftest
  (test "insert should return a sql string"
    (= "insert into account (name) values (:name)"
       (sql/insert :account {:name "name"})))

  (test "insert with multiple params"
    (= "insert into account (password, name) values (:password, :name)"
       (sql/insert :account {:name "name" :password "password"})))

  (test "insert with 'null param"
    (= "insert into account (name) values (:name)"
       (sql/insert :account {:name 'null})))

  (test "insert with multiple params with dashes"
    (is (deep= (->> (string/split " " "insert into account (password, created_at, name) values (:password, :created_at, :name)")
                    (map |(string/replace-all "(" "" $))
                    (map |(string/replace-all ")" "" $))
                    (map |(string/replace-all "," "" $))
                    (sorted))
               (->> (string/split " " (sql/insert :account {:name "name" :password "password" :created-at "created-at"}))
                    (map |(string/replace-all "(" "" $))
                    (map |(string/replace-all ")" "" $))
                    (map |(string/replace-all "," "" $))
                    (sorted)))))

  (test "insert-all test"
    (= "insert into account (name) values (?), (?)"
       (sql/insert-all :account [{:name "name1"} {:name "name2"}])))

  (test "insert-all test with two params"
    (= "insert into account (email, name) values (?,?), (?,?)"
       (sql/insert-all :account [{:name "name1" :email "email"} {:name "name2" :email "email2"}])))

  (test "insert-all test with kebab-cased params"
    (= "insert into table_a_b_c (email, a_b_c, name) values (?,?,?), (?,?,?)"
       (sql/insert-all :table-a-b-c [{:name "name1" :email "email" :a-b-c ""} {:name "name2" :email "email2" :a-b-c ""}])))

  (test "insert-all-params test with two params"
    (= ["email" "name1" "email2" "name2"]
       (freeze (sql/insert-all-params [{:name "name1" :email "email"} {:name "name2" :email "email2"}]))))

  (test "insert-all-params test with three params"
    (is (deep= (sorted ["email" 1 "name1" "email2" 2 "name2"])
               (sorted (sql/insert-all-params [{:name "name1" :email "email" :test 1} {:name "name2" :email "email2" :test 2}])))))

  (test "insert-all-params test"
    (= ["name1" "name2"]
       (freeze (sql/insert-all-params [{:name "name1"} {:name "name2"}]))))

  (test "insert-all-params test with nil"
    (= ["name1"]
       (freeze (sql/insert-all-params [{:name "name1"} {:name nil}]))))

  (test "update with dictionary where clause"
    (= "update account set name = :name where id = :id"
       (sql/update :account {:name "name"})))

  (test "update with string where clause"
    (= "update account set name = :name where id = :id"
       (sql/update :account {:name "name"})))

  (test "update with null value"
    (= "update account set name = null where id = :id"
       (sql/update :account {:name 'null})))

  (test "update with null value and kebab-case params"
    (= "update account set a_b_c = :a_b_c, name = null where id = :id"
       (sql/update :account {:name 'null :a-b-c ""})))

  (test "update-all test with same where and set keys"
    (= "update account set name = ? where name = ?"
       (sql/update-all :account {:name "old name"} {:name "new name"})))

  (test "update-all-params test"
    (= ["new name" "old name"]
       (freeze (sql/update-all-params {:name "old name"} {:name "new name"}))))

  (test "delete test"
    (= "delete from account where id = :id"
       (sql/delete :account 1)))

  (test "delete-all test with params dictionary"
    (= "delete from account where name = :name"
       (sql/delete-all :account {:where {:name "name"}})))

  (test "delete-all test with params dictionary kebab case"
    (= "delete from account where a_b_c = :a_b_c"
       (sql/delete-all :account {:where {:a-b-c ""}})))

  (test "delete-all test with params string"
    (= "delete from account where name = :name or name is null"
       (sql/delete-all :account {:where "name = :name or name is null"})))

  (test "where-clause test"
    (is (deep= (sorted (string/split " " "name = :name and id = :id"))
               (sorted (string/split " " (sql/where-clause {:id 1 :name "name"}))))))

  (test "where-clause with a null value"
    (is (deep= (sorted (string/split " " "name is null and id = :id"))
               (sorted (string/split " " (sql/where-clause {:id 1 :name 'null}))))))

  (test "from test"
    (is (= "select * from account where name = ?"
           (sql/from :account {:where {:name "name"}}))))

  (test "from with options test"
    (= "select * from account where name = ? order by rowid desc limit 3"
       (sql/from :account {:where {:name "name"} :order "rowid desc" :limit 3})))

  (test "from with join many (has-many) test"
    (= "select account.*, posts.id as 'posts/id', posts.name as 'posts/name' from account join posts on posts.account_id = account.id where name = ? order by rowid desc limit 3"
       (sql/from :account
                 {:join :posts :where {:name "name"} :order "rowid desc" :limit 3}
                 @["id" "name"]
                 @["id" "name"])))

  (test "from with join one (belongs-to) test"
    (= "select post.*, account.id as 'account/id', account.name as 'account/name' from post join account on account.id = post.account_id where name = ? order by rowid desc limit 3"
       (sql/from :post
                 {:join :account :where {:name "name"} :order "rowid desc" :limit 3}
                 @["account_id"]
                 @["id" "name"])))


  (test "fetch-options test with limit"
    (= "limit 10"
       (sql/fetch-options {:limit 10})))

  (test "fetch-options test with limit and offset"
    (= "limit 10 offset 2"
       (sql/fetch-options {:limit 10 :offset 2})))

  (test "fetch-options test with limit offset and order!"
    (= "order by name desc limit 10 offset 2"
       (sql/fetch-options {:limit 10 :offset 2 :order "name desc"})))

  (test "fetch-options test with limit offset and order asc"
    (= "order by name limit 10 offset 2"
       (sql/fetch-options {:limit 10 :offset 2 :order "name"})))

  (test "fetch-options test with limit offset and order by two args"
    (= "order by name, id desc limit 10 offset 2"
       (sql/fetch-options {:limit 10 :offset 2 :order "name, id desc"})))

  (test "fetch-options test with limit offset and order by with keyword"
    (= "order by name limit 10 offset 2"
       (sql/fetch-options {:limit 10 :offset 2 :order :name})))

  (test "clone-inside with one arg"
    (= [:a]
       (freeze (sql/clone-inside [:a]))))

  (test "clone-inside with two args"
    (= [:a :b]
       (freeze (sql/clone-inside [:a :b]))))

  (test "clone-inside with three args"
    (= [:a :b :b :c]
       (freeze (sql/clone-inside [:a :b :c]))))

  (test "join test"
    (= "join account on account.id = todo.account_id"
       (sql/join [:account :todo])))

  (test "fetch joins test with two tables"
    (= "join account on account.id = todo.account_id"
       (sql/fetch-joins [:account :todo])))

  (test "fetch joins test with three tables"
    (= "join todo on todo.id = comment.todo_id join account on account.id = todo.account_id"
       (sql/fetch-joins [:account :todo :comment])))

  (test "fetch test with one table no ids"
    (= "select account.* from account"
       (sql/fetch [:account])))

  (test "fetch test with one table and one id"
    (= "select account.* from account where account.id = ?"
       (sql/fetch [:account 1])))

  (test "fetch test with two tables and one id"
    (= "select todo.* from todo join account on account.id = todo.account_id where account.id = ?"
       (sql/fetch [:account 1 :todo])))

  (test "fetch test with two tables and two ids"
    (= "select todo.* from todo join account on account.id = todo.account_id where account.id = ? and todo.id = ?"
       (sql/fetch [:account 1 :todo 2])))

  (test "fetch test with two tables and two ids"
    (= "select todo.* from todo join account on account.id = todo.account_id where account.id = ? and todo.id = ?"
       (sql/fetch [:account 1 :todo 2])))

  (test "fetch test with three tables and two ids"
    (= "select comment.* from comment join todo on todo.id = comment.todo_id join account on account.id = todo.account_id where account.id = ? and todo.id = ?"
       (sql/fetch [:account 1 :todo 2 :comment])))

  (test "fetch-params test with three tables and two ids"
    (= [1 2]
       (freeze (sql/fetch-params [:account 1 :todo 2 :comment]))))

  (test "fetch-params test with two tables and one id"
    (= [1]
       (freeze (sql/fetch-params [:account 1 :todo]))))

  (test "fetch-params test with no ids"
    (= []
       (freeze (sql/fetch-params [:account]))))

  (test "fetch test with one table and options"
    (= "select account.* from account limit 10"
       (sql/fetch [:account] {:limit 10})))

  (test "fetch test with one table and limit and offset options"
    (= "select account.* from account limit 10 offset 2"
       (sql/fetch [:account] {:limit 10 :offset 2})))

  (test "where with a tuple"
    (is (= "select * from account where id = ?"
           (sql/from :account {:where ["id = ?" 1]}))))

  (test "find-by with a tuple"
    (is (= "select * from users where code = ?"
           (sql/from :users {:where ["code = ?" 123]})))))
