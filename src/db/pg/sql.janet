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
