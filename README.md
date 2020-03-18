# db
__A humble database library for janet__

## Install

```sh
jpm install https://github.com/joy-framework/db
```

You also need one of the following libs:

```sh
jpm install http://github.com/andrewchambers/janet-pq
# or
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

## Queries

You are now well versed in `db`. Happy coding!
