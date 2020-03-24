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
    (as-> ["select * from"
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
  [params]
  (if (string? params)
    params
    (do
      (var i 0)
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
