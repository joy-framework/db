(import ../helper :prefix "")


(defn insert
  "Returns an insert statement sql string from a dictionary"
  [table-name params]
  (let [columns (as-> (keys params) ?
                      (map snake-case ?)
                      (string/join ? ", "))
        vals (as-> (keys params) ?
                   (map snake-case ?)
                   (map |(string ":" $) ?)
                   (string/join ? ", "))
        sql-table-name (snake-case table-name)]
    (string "insert into " sql-table-name " (" columns ") values (" vals ") returning *")))


(defn fetch-options
  "Takes a dictionary and returns order by, limit and offset sql bits"
  [args]
  (when (not (nil? args))
    (let [{:order order :limit limit :offset offset} args
          order-by (when (not (nil? order)) (string "order by " order))
          limit (when (not (nil? limit)) (string "limit " limit))
          offset (when (not (nil? offset)) (string "offset " offset))]
      (as-> [order-by limit offset] ?
            (filter |(not (nil? $)) ?)
            (string/join ? " ")))))


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
         (filter |(not (nil? $)))
         (mapcat identity))))


(defn join
  "Returns a join statement from a tuple"
  [[left right]]
  (string "join " left " on " left ".id = " right "." left "_id"))


(defn fetch-joins
  "Returns several join strings from an array of keywords"
  [keywords]
  (when (> (length keywords) 1)
    (as-> (clone-inside keywords) ?
          (partition 2 ?)
          (map join ?)
          (reverse ?)
          (string/join ? " "))))


(defn fetch-params
  "Returns a table for a where clause of a 'fetch' sql string"
  [path]
  (filter |(not (keyword? $)) path))


(defn fetch
  "Takes a path and generates join statements along with a where clause."
  [path &opt args]
  (let [keywords (->> (filter keyword? path)
                      (map snake-case))
        ids (fetch-params path)
        where (when (not (empty? ids))
                (var i 0)
                (string "where "
                  (as-> (partition 2 path) ?
                        (filter |(= 2 (length $)) ?)
                        (map |(string (-> $ first snake-case) ".id = $" (++ i)) ?)
                        (string/join ? " and "))))]
    (as-> [(string/format "select %s.*" (last keywords))
           "from"
           (last keywords)
           (fetch-joins keywords)
           where
           (fetch-options args)] ?
          (filter string? ?)
          (string/join ? " "))))


(defn null? [val]
  (= 'null val))


(defn where-op
  "Takes kvs and returns $x where clause parameters"
  [[k v] counter]
  (var counter* counter)
  (if (= v 'null)
    "is null"
    (string "= $" (++ counter*))))


(defn where-clause
  "Takes either a string or a dictionary and returns a where clause with and or that same string"
  [params &opt initial-counter]
  (default initial-counter 0)
  (if (string? params)
    params
    (do
      (var i initial-counter)
      (as-> (pairs params) ?
            (map |(string (-> $ first snake-case) " " (where-op $ i)) ?)
            (string/join ? " and ")))))


(defn from
  "Takes a table name and where clause params and optional order/limit/offset options and returns a select sql string"
  [table-name &opt args]
  (let [where-params (get args :where)
        where (when (truthy? where-params)
               (string "where " (where-clause where-params)))]
    (as-> [(string "select * from " (snake-case table-name))
           where
           (fetch-options args)] ?
          (filter string? ?)
          (string/join ? " "))))


(defn insert-all
  "Returns a batch insert statement from an array of dictionaries"
  [table-name arr]
  (var i 0)
  (let [columns (as-> (first arr) ?
                      (keys ?)
                      (map snake-case ?)
                      (string/join ? ", "))
        vals (as-> (map keys arr) ?
                   (mapcat (fn [ks] (string "(" (string/join
                                                 (map (fn [_] (string "$" (++ i))) ks)
                                                 ",")
                                            ")")) ?)
                  (string/join ? ", "))]
    (string "insert into " (snake-case table-name) " (" columns ") values " vals " returning *")))


(defn update-param [[key val]]
  (let [column (snake-case key)
        value (if (= 'null val)
                "null"
                (string/format ":%s" column))]
    (string/format "%s = %s" column value)))


(defn update
  "Returns an update sql string from a dictionary of params representing the set portion of the update statement"
  [table-name params]
  (let [columns (as-> (pairs params) ?
                      (map update-param ?)
                      (string/join ? ", "))]
    (string "update " (snake-case table-name) " set " columns " where id = :id returning *")))


(defn update-all
  "Returns an update sql string from two dictionaries representing the where clause and the set clause"
  [table-name where-params set-params]
  (var i 0)
  (let [columns (as-> (pairs set-params) ?
                      (map |(string (first $) " = " (if (= 'null (last $))
                                                      "null"
                                                      (string "$" (++ i)))) ?)
                      (string/join ? ", "))]
    (string "update " (snake-case table-name) " set " columns " where " (where-clause where-params i)
            " returning *")))


(defn update-all-params
  "Returns an array of params for the update-all sql string"
  [where-params set-params]
  (array/concat
    (values set-params)
    (values where-params)))


(defn delete-all
  "Returns a delete sql string from a table name and value for the id column"
  [table-name params]
  (let [where-params (get params :where)
        where (when (truthy? where-params) (string "where " (where-clause where-params)))]
    (as-> [(string "delete from " (snake-case table-name) " returning *")
           where
           (fetch-options params)] ?
          (filter truthy? ?)
          (string/join ? " ")
          (string/trimr ?))))


(defn delete
  "Returns a delete sql string from a table name and value for the id column"
  [table-name]
  (string "delete from " (snake-case table-name) " where id = $1 returning *"))
