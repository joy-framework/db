(def database-url (os/getenv "DATABASE_URL"))
(def postgres? (string/has-prefix? "postgres" (or database-url "")))
(def sqlite? (not postgres?))


(defn file/write-all
  "Writes a new file with the given content"
  [filename &opt content]
  (default content "")
  (with [f (file/open filename :w)]
    (file/write f content)))


(defn file/touch
  "Creates a new empty file with the given filename"
  [filename]
  (file/write-all filename))


(defn file/read-all
  "Returns content of filename"
  [filename]
  (with [f (file/open filename :r)]
    (file/read f :all)))


(defn kebab-case
  `Changes a string from snake_case to kebab-case

   Example

   (kebab-case "created_at") -> "created-at"`
  [val]
  (string/replace-all "_" "-" val))


(defn snake-case
  `Changes a string from kebab-case to snake_case

   Example

   (snake-case "created-at") -> "created_at"`
  [val]
  (string/replace-all "-" "_" val))


(defn drop-last [val]
  (cond
    (array? val) (array/slice val 0 (dec (length val)))
    (tuple? val) (tuple/slice val 0 (dec (length val)))
    :else @[]))


(defn map-keys
  `Executes a function on a dictionary's keys and returns a table

   Example

   (map-keys snake-case {:created_at "" :uploaded_by ""}) -> {:created-at "" :uploaded-by ""}`
  [f dict]
  (let [acc @{}]
    (loop [[k v] :in (pairs dict)]
      (put acc (f k) v))
    acc))


(defn map-vals
  `Executes a function on a dictionary's values and returns a table

   Example

   (map-vals + {:a 1 :b 2}) -> {:a 2 :b 3}`
  [f dict]
  (let [acc @{}]
    (loop [[k v] :in (pairs dict)]
      (put acc k (f v)))
    acc))


(defn kebab-case-keys
  `Converts a dictionary with "snake_case_keys" to one with :kebab-case-keys

  Example:

  (kebab-case-keys @{"created_at" "now"}) => @{:created-at "now"}`
  [dict]
  (if (dictionary? dict)
    (as-> dict ?
          (map-keys kebab-case ?)
          (map-keys keyword ?))
    dict))


(defn snake-case-keys
  `Converts a dictionary with snake_case_keys to one with kebab-case-keys

  Example:

  (snake-case-keys @{"created-at" "now"}) => @{:created_at "now"}`
  [dict]
  (if (dictionary? dict)
    (as-> dict ?
          (map-keys snake-case ?)
          (map-keys keyword ?))
    dict))


(defn group-by [f ind]
  `Groups an indexed datastructure according to function f

  Example:

  (group-by |($ :tbl) [{:tbl "post" :col "id"} {:tbl "post" :col "created_at"}])

  =>

  @{"post" @[@{:tbl "post" :col "id"} @{:tbl "post" :col "created_at"}]}`
  (reduce
    (fn [ret x]
      (let [k (f x)]
        (put ret k (array/push (get ret k @[]) x))))
    @{} ind))


(defn contains?
  `Finds a truthy value in an indexed or a dictionary's
   keys

   Example

   (contains? :a [:a :b :c]) => true
   (contains? :a {:a 1 :b 2 :c 3}) => true
   (contains? :d {:a 1 :b 2 :c 3}) => false`
  [val arr]
  (when (or (indexed? arr)
            (dictionary? arr))
    (truthy?
     (or (find |(= val $) arr)
         (get arr val)))))


(defn table/slice
  `Selects part of a dictionary based on ind of keys. Returns a table.

   Example:

   (table/slice @{:a 1 :b 2 :c 3} [:a :b]) => @{:a 1 :b 2}
   (table/slice @{:a 1 :b 2 :c 3} []) => @{}
   (table/slice @{:a 1 :b 2 :c 3} [:a]) => @{:a 1}`
  [dict ind]
  (->> (pairs dict)
       (filter |(contains? (first $) ind))
       (mapcat identity)
       (apply table)))


(defn plural [str]
  (let [patterns [["is" "es"]
                  ["alias" "aliases"]
                  ["status" "statuses"]
                  ["us" "i"]
                  ["sis" "ses"]
                  ["o" "oes"]
                  ["y" "ies"]
                  ["tch" "tches"]
                  ["ize" "izes"]
                  ["ex" "ices"]
                  ["ix" "ices"]
                  ["x" "xes"]
                  ["mouse" "mice"]
                  ["s" "s"]
                  ["" "s"]]]
    (var s str)
    (loop [[suffix subst] :in patterns]
      (when (string/has-suffix? suffix s)
        (do
          (set s (string/trimr s suffix))
          (set s (string s subst))
          (break))))
    s))


(defn singular [str]
  (let [patterns [["ies" "y"]
                  ["tches" "tch"]
                  ["esses" "ess"]
                  ["sses" "ss"]
                  ["es" "e"]
                  ["s" ""]]]
    (var s str)
    (loop [[suffix subst] :in patterns]
      (when (string/has-suffix? suffix s)
        (do
          (set s (string/trimr s suffix))
          (set s (string s subst))
          (break))))
    s))


(defn present? [val]
  (and val (not (empty? val))))


(defn blank? [val]
  (not (present? val)))


(defn dissoc [dict & ks]
  (var t (merge-into @{} dict))

  (loop [k :in ks]
    (put t k nil))

  t)


(defn collect [ind & ks]
  (->> (map |(table/slice $ ks) ind)
       (map values)))
