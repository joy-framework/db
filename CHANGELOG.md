## db 0.4.0 (09/12/2020)

Don't call it a rewrite (*Breaking Changes*)

This commit was almost completely backwards compatible.

One function changed signature because it was super confusing,

update-all changed from

(update-all :post {:id [1 2 3]} {:name "new name"})

to

(update-all :post :set {:name "new name"} :where {:id [1 2 3]})

A bunch of other things changed too, a lot of functions are now
private that should have been before, so that's technically a
breaking change.

The biggest feature that comes out of all this is multi table join,
join/one and join/many!

One less reason to drop down to sql!

* 1a16723 - Don't call it a rewrite *Breaking Changes*
* d077913 - Watch files and re-run tests
* 690ad1e - Add indexed support to where clauses
* c8cdb2f - Maintain order when using join/one join/many

## db 0.3.0 (06/25/2020)

* 7f0442f - Update README <Sean Walker>
* 5843b6a - Add join, join/one and join/many support to sqlite from and find-by fns <Sean Walker>
* 7602a12 - Add singular/plural helpers <Sean Walker>
* 5efa3ea - Add contains? and table/slice helpers <Sean Walker>
* 4496158 - Add :db/table to rest of postgres functions <Sean Walker>
* 19c421c - Add :db/table to rest of sqlite functions <Sean Walker>

## db 0.2.0 (06/24/2020)

* 97b06a7 - Add single argument versions of insert/update/delete <Sean Walker>
* a05df90 - Make db schema return whole database schema. Check for updated-at col <Sean Walker>
* de48571 - Add group-by function <Sean Walker>
* d979aeb - Get update working. Change execute/query to handle [] or {} params <Sean Walker>
* 8ed5bc4 - Fix returned rows from sqlite insert-all <Sean Walker>
* 88a90e8 - Add row/val/all functions to sqlite api <Sean Walker>
* 7fbd37e - Update docs to show arrays/tables returned instead of tuples/structs <Sean Walker>
* af010a3 - Add sqlite db-test <Sean Walker>
* ce2dac2 - Add missing postgres functions <Sean Walker>
* f0f9c94 - Don't write schema file in prod <Sean Walker>
* 21bf3ac - Only run postgres tests when there is a postgres server running <Sean Walker>
* acfb0f8 - Fix fetch to only return columns from fetched table <Sean Walker>
* 495e59b - Merge pull request #1 from katafrakt/pg-same-api <Sean Walker>
* 6402076 - Add optional database url param to pg/connect <Sean Walker>
* c001ca3 - Unify function signatures of sqlite3 and pg <Paweł Świątkowski>


## db 0.1.0 (03/12/2020)

* First release! 🎉
