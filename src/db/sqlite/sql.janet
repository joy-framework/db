(import ../helper :prefix "")


(defn where-op
  "Takes kvs and returns either ? or :name params as strings in a where clause"
  [[k v] &opt positional?]
  (cond
    (= v 'null) "is null"
    (indexed? v) (string/format "in (%s)"
                                (string/join (map (fn [_] "?") v) ","))
    :else (if positional?
            (string "= ?")
            (string "= :" (snake-case k)))))


(defn where-clause
  "Takes either a string or a dictionary and returns a where clause with and or that same string"
  [params &opt positional?]
  (if (string? params)
    params
    (as-> (pairs params) ?
          (map |(string (-> $ first snake-case) " " (where-op $ positional?)) ?)
          (string/join ? " and "))))


(defn fetch-options
  "Takes a dictionary and returns order by, limit and offset sql bits"
  [args]
  (when (not (nil? args))
    (let [{:order order :limit limit :offset offset} args
          order-by (when order (string "order by " order))
          limit (when limit (string "limit " limit))
          offset (when offset (string "offset " offset))]
      (as-> [order-by limit offset] ?
            (filter string? ?)
            (string/join ? " ")))))


(defn from
  "Takes a table name and where clause params and optional order/limit/offset options and returns a select sql string"
  [table-name &opt args]
  (let [where-params (get args :where)
        where (when (not (nil? where-params)) (string "where " (where-clause where-params true)))]
    (as-> [(string "select * from " (snake-case table-name))
           where
           (fetch-options args)] ?
          (filter string? ?)
          (string/join ? " "))))


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
  (->> (filter |(not (keyword? $)) path)
       (map |(if (dictionary? $)
               (get $ :id)
               $))))


(defn fetch
  "Takes a path and generates join statements along with a where clause. Think 'get-in' for sqlite."
  [path &opt args]
  (let [keywords (->> (filter keyword? path)
                      (map snake-case))
        ids (fetch-params path)
        where (when (not (empty? ids))
                (string "where "
                  (as-> (partition 2 path) ?
                        (filter |(= 2 (length $)) ?)
                        (map |(string (-> $ first snake-case) ".id = ?") ?)
                        (string/join ? " and "))))]
    (as-> ["select * from"
           (last keywords)
           (fetch-joins keywords)
           where
           (fetch-options args)] ?
          (filter string? ?)
          (string/join ? " "))))


(defn insert
  "Returns an insert statement sql string from a dictionary"
  [table-name params]
  (let [columns (as-> (keys params) ?
                      (map snake-case ?)
                      (string/join ? ", "))
        vals (as-> (keys params) ?
                   (map snake-case ?)
                   (map |(string ":" $) ?)
                   (string/join ? ", "))]
    (string "insert into " (snake-case table-name) " (" columns ") values (" vals ")")))


(defn insert-all
  "Returns a batch insert statement from an array of dictionaries"
  [table-name arr]
  (let [columns (as-> (first arr) ?
                      (keys ?)
                      (map snake-case ?)
                      (string/join ? ", "))
        vals (as-> (map keys arr) ?
                   (mapcat (fn [ks] (string "(" (string/join
                                                 (map (fn [_] (string "?")) ks)
                                                 ",")
                                            ")")) ?)
                  (string/join ? ", "))]
    (string "insert into " (snake-case table-name) " (" columns ") values " vals)))


(defn insert-all-params
  "Returns an array of values from an array of dictionaries for the insert-all sql string"
  [arr]
  (mapcat values arr))


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
    (string "update " (snake-case table-name) " set " columns " where id = :id")))


(defn update-all
  "Returns an update sql string from two dictionaries representing the where clause and the set clause"
  [table-name where-params set-params]
  (let [columns (as-> (pairs set-params) ?
                      (map |(string (first $) " = " (if (= 'null (last $))
                                                      "null"
                                                      (string "?"))) ?)
                      (string/join ? ", "))]
    (string "update " (snake-case table-name) " set " columns " where " (where-clause where-params true))))


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
    (as-> [(string "delete from " (snake-case table-name))
           where
           (fetch-options params)] ?
          (filter truthy? ?)
          (string/join ? " ")
          (string/trimr ?))))


(defn delete
  "Returns a delete sql string from a table name and value for the id column"
  [table-name id]
  (string/trimr
   (delete-all table-name {:where {:id id}})))
