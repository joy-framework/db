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