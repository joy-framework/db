(import ../helper :prefix "")


(defn- ?param [val]
  "?")


(defn- ?params [dict]
  (as-> (keys dict) ?
        (map ?param ?)
        (string/join ? ", ")))


(defn- pk? [val]
  (when val
    (= "id" (string val))))


(defn- null? [val]
  (or (= 'null val)
      (= :null val)))


(defn- nilify [val]
  (if (null? val)
    nil
    val))


(defn- where/op
  "Takes tuples and returns a where clause 'part'"
  [[k v]]
  (cond
    (null? v) "is null"
    (indexed? v) (string/format "in (%s)"
                                (string/join (map (fn [_] "?") v) ","))
    :else (string "= ?")))


(defn- where
  "Takes a string, indexed or dictionary and returns a where clause"
  [params]
  (def params (get params :where))

  (def s (cond
            (indexed? params)
            (first params)

            (dictionary? params)
            (as-> (pairs params) ?
                  (map |(string (-> $ first snake-case) " " (where/op $)) ?)
                  (string/join ? " and "))

            :else params))

  (when s
    (string "where " s)))


(defn- sql-params [val]
  (cond
    (dictionary? val)
    (->> (values val)
         (mapcat identity)
         (map nilify))

    (indexed? val)
    (->> (drop 1 val)
         (map nilify))

    :else []))


(defn- where/params [p]
  (->> (get p :where)
       (sql-params)
       (filter (comp not nil?))))


(defn- join/tables [args]
  (def t (or (get args :join)
             (get args :join/one)
             (get args :join/many)))

  (cond
    (nil? t)
    []

    (indexed? t)
    (map string t)

    :else [(string t)]))


(defn- join? [args]
  (any? (join/tables args)))


(defn- join/columns [schema tables]
  (->> (table/slice schema tables)
       (mapcat values)))


(defn- join/select [args schema]
  (def cols (->> (join/tables args)
                 (join/columns schema)))

  (as-> (join/tables args) ?
        (join/columns schema ?)
        (map |(string/format "%s as '%s'" $ (string/replace "." "/" $)) ?)
        (string/join ? ", ")))


(defn- join/line [from-table join-table schema]
  (when join-table
    (def join-type (if (contains? (string from-table "." join-table "_id") (get schema from-table))
                     :one
                     :many))

    (def join-table (snake-case join-table))
    (def from-table (snake-case from-table))
    (case join-type
      :one
      (string/format "join %s on %s.id = %s.%s_id"
                     join-table join-table from-table join-table)

      :many
      (string/format "join %s on %s.%s_id = %s.id"
                     join-table join-table from-table from-table))))


(defn- join [table-name args schema]
  (as-> (join/tables args) ?
        (map |(join/line table-name $ schema) ?)
        (string/join ? " ")))


(defn- select [table-name args schema]
  (if (join? args)
    (string "select " table-name ".*, " (join/select args schema))
    (string "select *")))


(defn- order-by [args]
  (when-let [s (get args :order)]
    (string "order by " s)))


(defn- limit [args]
  (when-let [s (get args :limit)]
    (string "limit " s)))


(defn- offset [args]
  (when-let [s (get args :offset)]
    (string "offset " s)))


(defn from
  "Takes a table name and where clause params and optional order/limit/offset and returns a select sql string"
  [table-name &opt args schema]
  (default args {})
  (default schema [])

  (let [table-name (snake-case table-name)

        sql (as-> [(select table-name args schema) # select statement
                   (string "from " table-name) # from
                   (join table-name args schema) # join
                   (where args) # where
                   (order-by args) # order by
                   (limit args) # limit
                   (offset args)] ? # offset
                  (filter present? ?)
                  (string/join ? " ")
                  (string/trim ?))

        params (where/params args)]

    [sql ;params]))


(defn clone-inside
  "Copies the inner elements of arrays when there are three or more elements"
  [an-array]
  (let [first-val (first an-array)
        inner (drop 1 an-array)
        last-val (when (not (empty? inner))
                   (last inner))
        inner (drop-last inner)
        cloned-inner (interleave inner inner)]
    (->> [first-val cloned-inner last-val]
         (filter truthy?)
         (mapcat identity))))


(defn fetch/join
  "Returns a join statement from a tuple"
  [[left right]]
  (string "join " left " on " left ".id = " right "." left "_id"))


(defn fetch/joins
  "Returns several join strings from an array of keywords"
  [keywords]
  (when (> (length keywords) 1)
    (as-> (clone-inside keywords) ?
          (partition 2 ?)
          (map fetch/join ?)
          (reverse ?)
          (string/join ? " "))))


(defn fetch/params
  "Returns a table for a where clause of a 'fetch' sql string"
  [path]
  (->> (filter |(not (keyword? $)) path)
       (map |(if (dictionary? $)
               (get $ :id)
               $))))


(defn fetch
  "Takes a path and generates join statements along with a where clause. Think 'get-in' for sqlite."
  [path &opt args]
  (let [keywords (->> (filter keyword? path)
                      (map snake-case))
        ids (fetch/params path)
        where (unless (empty? ids)
                (string "where "
                  (as-> (partition 2 path) ?
                        (filter |(= 2 (length $)) ?)
                        (map |(string (-> $ first snake-case) ".id = ?") ?)
                        (string/join ? " and "))))
        sql (as-> [(string/format "select %s.*" (last keywords))
                   (string "from " (last keywords))
                   (fetch/joins keywords)
                   where
                   (order-by args)
                   (limit args)
                   (offset args)] ?
                  (filter present? ?)
                  (string/join ? " "))]

    [sql ;ids]))


(defn- set-param [key]
  (string (snake-case key) " = ?"))


(defn- on-conflict-clause [options]
  (let [{:do do* :on-conflict on-conflict :update update* :set set-columns} options
        conflict-columns (if (indexed? on-conflict)
                           on-conflict
                           [on-conflict])
        conflict-columns-str (string "( " (string/join conflict-columns ", ") ")")
        update* (when (= do* :update)
                  (let [set-columns-str (-> (map set-param (keys set-columns))
                                            (string/join ", "))]
                    (string "set " set-columns-str)))]

    (as-> ["on conflict" conflict-columns-str "do" do* update*] ?
          (filter present? ?)
          (string/join ? " "))))


(defn insert
  "Returns an insert statement sql string from a dictionary"
  [table-name params &opt options]
  (default options {})

  (let [columns (as-> (keys params) ?
                      (map snake-case ?)
                      (string/join ? ", "))
        vals (?params params)
        sql (string "insert into " (snake-case table-name) " (" columns ") values (" vals ")")
        sql (if (options :on-conflict)
              (string sql " " (on-conflict-clause options))
              sql)]
    [sql ;(array/concat (sql-params params) (if (options :set) (sql-params (options :set)) @[]))]))


(defn insert-all
  "Returns a batch insert statement from an array of dictionaries"
  [table-name arr]
  (let [columns (as-> (first arr) ?
                      (keys ?)
                      (map snake-case ?)
                      (string/join ? ", "))
        vals (as-> (map ?params arr) ?
                   (map |(string/format "(%s)" $) ?)
                   (string/join ? ", "))]
    [(string "insert into " (snake-case table-name) " (" columns ") values " vals)
     ;(mapcat sql-params arr)]))


(defn update
  "Returns an update sql string from a dictionary of params representing the set portion of the update statement"
  [table-name set-params where-params]
  (let [columns (as-> (keys set-params) ?
                      (filter (comp not pk?) ?)
                      (map set-param ?)
                      (string/join ? ", "))
        sql (string "update " (snake-case table-name) " set " columns " " (where {:where where-params}))
        set-params (sql-params set-params)
        where-params (->> (sql-params where-params)
                          (filter (comp not nil?)))]
    [sql ;set-params ;where-params]))


(defn update-all
  "Returns an update sql string from two dictionaries representing the where clause and the set clause"
  [table-name set-params where-params]
  (update table-name set-params where-params))


(defn delete-all
  "Returns a delete sql string from a table name and value for the id column"
  [table-name params]
  (def sql (as-> [(string "delete from " (snake-case table-name))
                  (where params)
                  (order-by params)
                  (limit params)
                  (offset params)] ?
                 (filter present? ?)
                 (string/join ? " ")))

  [sql ;(where/params params)])


(defn delete
  "Returns a delete sql string from a table name and value for the id column"
  [table-name id]
  (delete-all table-name {:where {:id id}}))
