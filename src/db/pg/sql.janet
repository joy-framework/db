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
