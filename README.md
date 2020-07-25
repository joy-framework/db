# db
__A humble database library for janet__

## Install

```sh
jpm install https://github.com/joy-framework/db
```

You also need one of the following libs for a database driver:

```sh
# for postgres
jpm install http://github.com/andrewchambers/janet-pq

# or this for sqlite
jpm install http://github.com/janet-lang/sqlite3
```

After installing, it should create an executable janet file named `db` in `(dyn :syspath)/bin`
Make sure that directory is in your `PATH`

## Usage

db reads your current os environment for the connection string in the variable `DATABASE_URL`, which can either start with `postgres` or if it doesn't, db defaults to sqlite3.

```sh
# your environment variables

# for postgres
DATABASE_URL=postgres://user3123:passkja83kd8@ec2-117-21-174-214.compute-1.amazonaws.com:6212/db982398

# for sqlite
DATABASE_URL=db982398.sqlite3
```

## Create a database

db supports two databases

1. sqlite
2. postgres

### sqlite

Run this command to create a sqlite database in the current directory

```sh
db create database:sqlite todos_dev.sqlite3
```

`todos_dev.sqlite3` can be any name

### postgres

Run this command to create a postgres database, assuming a running postgres server and a `createdb` cli script in the current `PATH`

```sh
db create database:postgres todos_dev
```

## Migrations

Creating migrations happens with the same cli program which should get installed when you run `jpm install`

Note: Make sure you have the janet module `bin` folder in your `PATH`.

```sh
db create migration 'create-table-todos'
```

This should create a new folder in your current directory named `db/migrations` and in that folder, there should be an empty `.sql` file named `<long-number>-create-table-todos.sql`:

```sql
-- up:
-- down:
```

We can do a little better than that though:

```sh
db create table 'todos' 'name text not null' 'completed-at datetime'
```

This should create a new sql file that looks like this:

```sql
-- up:
create table todos (
  name text not null,
  completed_at datetime
);

-- down:
drop table todos;
```

Kebab-case gets converted to snake_case automatically.

Run that migration:

```sh
db migrate
```

Roll that migration back just because

```sh
db rollback
```

## Connecting to the database

```clojure
(import db)

(db/connect (os/getenv "DATABASE_URL"))

; # or

(db/connect) ; # uses the DATABASE_URL environment variable implicitly

(db/disconnect)
```

## CRUD

Given a table that looks like this:

```sql
create table todos (
  id integer primary key,
  name text
);
```

__insert__
```clojure
(db/insert {:db/table :todos :name "mow the lawn"}) ; # => {:name "mow the lawn" :db/table :todos :id 1}
; # or
(db/insert :todos {:name "mow the lawn"}) ; # => {:name "mow the lawn" :db/table :todos :id 1}
```

__update__
```clojure
(db/update :todos {:id 1 :name "mow the lawn!"}); # => {:name "mow the lawn!" :db/table :todos :id 1}
; # or
(db/update {:db/table :todos :id 1 :name "mow the lawn!"}); # => {:name "mow the lawn!" :db/table :todos :id 1}
```

__delete__
```clojure
(db/delete {:db/table :todos :id 1}) ; # => {:name "mow the lawn!" :db/table :todos :id 1}
; # or
(db/delete :todos 1) ; # => {:name "mow the lawn!" :db/table :todos :id 1}
```

## Queries

There are a few ways in db to do queries, the first way is to find a row by primary key

```clojure
(db/find :todos 1) ; # => {:name "mow the lawn!" :db/table :todos :id 1}
```

Another way is to "fetch" by primary key, the main difference being is that you can "scope" things by foreign key.

```clojure
(db/fetch [:todos 1]) ; # => {:name "mow the lawn!" :db/table :todos :id 1}
```

Let's say there was an accounts table and you wanted to get only the todos for a given account row, so with this schema:

```sql
create table accounts (
  id integer primary key,
  name text
);

create table todos (
  id integer primary key,
  accounts_id integer references accounts(id),
  name text
);
```

You would get the todos by account like so:

```clojure
(db/insert :accounts {:name "account #1"})
(db/insert :accounts {:name "account #2"})

(db/insert :todos {:name "todo #1" :account-id 1})
(db/insert :todos {:name "todo #2" :account-id 1})
(db/insert :todos {:name "todo #3" :account-id 2})
(db/insert :todos {:name "todo #4" :account-id 2})

(db/fetch [:accounts 1 :todos 1]) ; # return only the todo row for that account
```

To return all the rows from an account, use `fetch-all`

```clojure
(db/fetch-all [:accounts 1 :todos]) ; # returns all todos for that account

; # you can apply sql options like so:

(db/fetch-all [:accounts 1 :todos] :limit 10 :offset 0 :order "todos.id desc")
```

That's scoping in db. The next trick is a more flexible way of querying, not by primary key, `from`

```clojure
(db/from :accounts
         :where ["name like ?" "%#1"]) ; # => [{:id 1 :name "account #1"}]
```

The same thing applies there with the sql options, `:limit`, `:order`, `:offset` and `:join` should all work. Here's another example with a few options:

```clojure
(db/from :accounts
         :join :todos
         :where ["id = ?" 1])

; # => [{:id 1 :name "account #1" :todos/id 2 :todos/name "todos #2" ...} ...]
```

There's one more thing that make `from` and `find-by` a little special, `:join/one` and `:join/many`:

```clojure
(db/from :accounts :join/many :todos)

; # returns

[{:id 1
  :name "account #1"
  :todos [{:id 1
           :name "todo #1"}
          {:id 2
           :name "todo #2"}]}]
```

There are a few other things db can do that haven't been documented, check the tests for a more complete look if you're interested.
