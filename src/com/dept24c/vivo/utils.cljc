(ns com.dept24c.vivo.utils
  (:require
   #?(:cljs [cljs.reader :as reader])
   #?(:clj [clojure.edn :as edn])
   #?(:cljs [clojure.pprint :as pprint])
   [clojure.string :as str]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.logging :as logging]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.stockroom :as sr]
   #?(:clj [puget.printer :refer [cprint cprint-str]]))
  #?(:cljs
     (:require-macros
      [com.dept24c.vivo.utils :refer [sym-map]])))

#?(:clj
   (set! *warn-on-reflection* true))

(def all-branches-reference "_ALL_BRANCHES")
(def alphanumeric-chars-w-hyphen
  (-> (seq "-ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789")
      (set)))
(def branch-reference-root "_BRANCH_")
(def fp-to-schema-reference-root "_FP_")
(def max-branch-name-len 64)

(defmacro sym-map
  "Builds a map from symbols.
   Symbol names are turned into keywords and become the map's keys.
   Symbol values become the map's values.
  (let [a 1
        b 2]
    (sym-map a b))  =>  {:a 1 :b 2}"
  [& syms]
  (zipmap (map keyword syms) syms))

(defprotocol IDataStorage
  (<delete-reference! [this reference])
  (<get-in [this data-id schema path])
  (<get-in-reference [this reference schema path])
  (<update
    [this data-id schema update-commands]
    [this data-id schema update-commands tx-fns])
  (<update-reference!
    [this reference schema update-commands]
    [this reference schema update-commands tx-fns]))

(defprotocol IDataBlockStorage
  (<schema->fp [this schema])
  (<fp->schema [this fp])
  (<allocate-data-block-id [this])
  (<compare-and-set! [this reference schema old new])
  (<read-data-block [this block-id schema])
  (<write-data-block [this block-id schema data])
  (<delete-data-block [this block-id])
  (<set-reference! [this reference data-id])
  (<get-data-id [this reference]))

(defprotocol IBlockStorage
  (<allocate-block-id [this])
  (<compare-and-set-bytes! [this reference old-bytes new-bytes])
  (<delete-block [this block-id])
  (<read-block
    [this block-id]
    [this block-id skip-cache?])
  (<write-block
    [this block-id bytes]
    [this block-id bytes skip-cache?]))

(defn configure-capsule-logging
  ([]
   (configure-capsule-logging :debug))
  ([level]
   (logging/add-log-reporter! :println logging/println-reporter)
   (logging/set-log-level! level)))

(defn pprint [x]
  #?(:clj (.write *out* ^String (str (cprint-str x) "\n"))
     :cljs (pprint/pprint x)))

(defn pprint-str [x]
  #?(:clj (cprint-str x)
     :cljs (with-out-str (pprint/pprint x))))

(defn ex-msg [e]
  #?(:clj (.toString ^Exception e)
     :cljs (.-message e)))

(defn ex-stacktrace [e]
  #?(:clj (clojure.string/join "\n" (map str (.getStackTrace ^Exception e)))
     :cljs (.-stack e)))

(defn ex-msg-and-stacktrace [e]
  (str "\nException:\n" (ex-msg e) "\nStacktrace:\n" (ex-stacktrace e)))

(defn current-time-ms []
  #?(:clj (System/currentTimeMillis)
     :cljs (.getTime (js/Date.))))

(defn edn->str [edn]
  (pr-str edn))

(defn str->edn [s]
  #?(:clj (edn/read-string s)
     :cljs (reader/read-string s)))

(defn ms->mins [ms]
  (int (/ ms 1000 60)))

(defn str->int [s]
  (when-not (string? s)
    (if (nil? s)
      (throw (ex-info "Argument to str->int must not be nil."
                      {}))
      (throw (ex-info (str "Argument to str->int must be a string. Got: "
                           (type s))
                      {:arg s
                       :arg-type (type s)}))))
  (when-not (seq s)
    (throw (ex-info "Argument to str->int must not be the empty string."
                    {})))
  (try
    #?(:clj (Integer/parseInt s)
       :cljs (js/parseInt s 10))
    (catch #?(:clj Exception :cljs js/Error) e
      (throw (ex-info (str "Could not convert " s " to an integer.")
                      {:e e
                       :arg s})))))

(defn str->float [s]
  (when-not (string? s)
    (if (nil? s)
      (throw (ex-info "Argument to str->float must not be nil."
                      {}))
      (throw (ex-info (str "Argument to str->float must be a string. Got: "
                           (type s))
                      {:arg s
                       :arg-type (type s)}))))
  (when-not (seq s)
    (throw (ex-info "Argument to str->float must not be the empty string."
                    {})))
  (try
    #?(:clj (Float/parseFloat s)
       :cljs (js/parseFloat s 10))
    (catch #?(:clj Exception :cljs js/Error) e
      (throw (ex-info (str "Could not convert " s " to a float")
                      {:e e
                       :arg s})))))

(defn relationship-info
  "Given two key sequences, return a vector of [relationship tail].
   Relationsip is one of :sibling, :parent, :child, or :equal.
   Tail is the keypath between the parent and child. Only defined when
   relationship is :parent."
  [ksa ksb]
  (let [va (vec ksa)
        vb (vec ksb)
        len-a (count va)
        len-b (count vb)
        len-min (min len-a len-b)
        divergence-i (loop [i 0]
                       (if (and (< i len-min)
                                (= (va i) (vb i)))
                         (recur (inc i))
                         i))
        a-tail? (> len-a divergence-i)
        b-tail? (> len-b divergence-i)]
    (cond
      (and a-tail? b-tail?) [:sibling nil]
      a-tail? [:child nil]
      b-tail? [:parent (drop divergence-i ksb)]
      :else [:equal nil])))

(defn check-reference [reference]
  (when (or (not (string? reference))
            (not (str/starts-with? reference "_")))
    (throw (ex-info (str "Bad reference: `" reference "`. References must "
                         "be strings that start with an underscore.")
                    {:given-reference reference}))))

(defn check-data-id [id]
  (when (or (not (string? id))
            (not (alphanumeric-chars-w-hyphen (first id))))
    (throw (ex-info (str "Bad data-id `" id "`. All data-ids must "
                         "be strings that start with a letter, a number, "
                         "or a hyphen (for temp data-ids)")
                    {:given-data-id id}))))

;;;;;;;;;;;;;;;;;;;; Schemas ;;;;;;;;;;;;;;;;;;;;

(def block-num-schema l/long-schema)
(def branch-name-schema l/string-schema)
(def data-id-schema l/string-schema)
(def fp-schema l/long-schema)
(def identifier-schema l/string-schema)
(def secret-schema l/string-schema)
(def subject-id-schema l/string-schema)
(def token-schema l/string-schema)
(def valid-ops  [:set :remove :insert-before :insert-after
                 :plus :minus :multiply :divide :mod])

(def all-branches-schema (l/array-schema branch-name-schema))
(def op-schema (l/enum-schema :com.dept24c.vivo.utils/op
                              valid-ops))

(l/def-map-schema string-map-schema
  l/string-schema)

(l/def-record-schema db-info-schema
  [:data-id data-id-schema]
  [:subject-id-to-hashed-secret-data-id data-id-schema]
  [:identifier-to-subject-id-data-id data-id-schema]
  [:token-to-token-info-data-id data-id-schema]
  [:subject-id-to-tokens-data-id data-id-schema]
  [:msg l/string-schema]
  [:timestamp-ms l/long-schema]
  [:num-prev-dbs l/int-schema]
  [:prev-db-id data-id-schema])

(l/def-union-schema path-item-schema
  l/keyword-schema
  l/string-schema
  l/long-schema)

(l/def-array-schema path-schema
  path-item-schema)

(l/def-record-schema serialized-value-schema
  [:fp (l/maybe l/long-schema)]
  [:bytes (l/maybe l/bytes-schema)])

(l/def-record-schema serializable-update-command-schema
  [:path (l/maybe path-schema)]
  [:op op-schema]
  [:arg (l/maybe serialized-value-schema)])

(def serializable-update-commands-schema
  (l/array-schema serializable-update-command-schema))

(def db-id-schema l/string-schema)

(l/def-record-schema get-state-arg-schema
  [:db-id db-id-schema]
  [:path path-schema])

(l/def-enum-schema unauthorized-schema
  :vivo/unauthorized)

(def get-state-ret-schema
  (l/union-schema [l/null-schema unauthorized-schema serialized-value-schema]))

(l/def-record-schema sys-state-change-schema
  [:cur-db-id db-id-schema]
  [:prev-db-id db-id-schema]
  [:updated-paths (l/array-schema (l/maybe path-schema))])

(l/def-record-schema log-in-arg-schema
  [:identifier identifier-schema]
  [:secret secret-schema])

(l/def-record-schema log-in-ret-schema
  [:subject-id subject-id-schema]
  [:token token-schema])

(l/def-record-schema add-subject-arg-schema
  [:identifier identifier-schema]
  [:secret secret-schema]
  [:subject-id subject-id-schema])

(l/def-record-schema token-info-schema
  [:expiration-time-mins l/long-schema]
  [:subject-id subject-id-schema])

(def token-map-schema (l/map-schema token-info-schema))

(def subject-id-to-tokens-schema
  (l/map-schema (l/array-schema token-schema)))

(l/def-record-schema subject-info-schema
  [:hashed-secret l/string-schema]
  [:identifiers (l/array-schema l/string-schema)])

(l/def-record-schema branch-state-source-schema
  [:branch/name l/string-schema])

(l/def-record-schema temp-branch-state-source-schema
  [:temp-branch/db-id (l/maybe db-id-schema)])

(def state-source-schema (l/union-schema [branch-state-source-schema
                                          temp-branch-state-source-schema]))

(l/def-record-schema create-branch-arg-schema
  [:branch branch-name-schema]
  [:db-id db-id-schema])

(def sm-server-protocol
  {:roles [:state-manager :server]
   :msgs {:add-subject {:arg add-subject-arg-schema
                        :ret subject-id-schema
                        :sender :state-manager}
          :add-subject-identifier {:arg l/string-schema
                                   :ret l/boolean-schema
                                   :sender :state-manager}
          :change-secret {:arg l/string-schema
                          :ret l/boolean-schema
                          :sender :state-manager}
          :get-schema-pcf {:arg l/long-schema
                           :ret (l/maybe l/string-schema)
                           :sender :either}
          :get-state {:arg get-state-arg-schema
                      :ret get-state-ret-schema
                      :sender :state-manager}
          :log-in {:arg log-in-arg-schema
                   :ret (l/maybe log-in-ret-schema)
                   :sender :state-manager}
          :log-in-w-token {:arg token-schema
                           :ret (l/maybe subject-id-schema)
                           :sender :state-manager}
          :log-out {:arg l/null-schema
                    :sender :state-manager}
          :set-state-source {:arg state-source-schema
                             :ret db-id-schema
                             :sender :state-manager}
          :sys-state-changed {:arg sys-state-change-schema
                              :sender :server}
          ;; We need to return the state change so sys+local updates
          ;; remain transactional
          :update-state {:arg serializable-update-commands-schema
                         :ret (l/maybe sys-state-change-schema)
                         :sender :state-manager}}})

(def admin-client-server-protocol
  {:roles [:admin-client :server]
   :msgs {:create-branch {:arg create-branch-arg-schema
                          :ret l/boolean-schema
                          :sender :admin-client}
          :delete-branch {:arg branch-name-schema
                          :ret l/boolean-schema
                          :sender :admin-client}}})

(defn get-undefined-syms [sub-map]
  (let [defined-syms (set (keys sub-map))]
    (vec (reduce-kv (fn [acc sym path]
                      (cond
                        (sequential? path)
                        (reduce (fn [acc* k]
                                  (if-not (symbol? k)
                                    acc*
                                    (if (defined-syms k)
                                      acc*
                                      (conj acc* k))))
                                acc path)

                        (= :vivo/subject-id path)
                        acc

                        :else
                        (throw (ex-info
                                (str "Bad path. Must be a sequence or "
                                     ":vivo/subject-id.")
                                (sym-map sym path sub-map)))))
                    #{} sub-map))))

(defn check-sub-map
  [subscriber-name subscriber-type sub-map]
  (when-not (map? sub-map)
    (throw (ex-info "The sub-map parameter must be a map."
                    (sym-map sub-map))))
  (when-not (pos? (count sub-map))
    (throw (ex-info "The sub-map parameter must contain at least one entry."
                    (sym-map sub-map))))
  (let [undefined-syms (get-undefined-syms sub-map)]
    (when (seq undefined-syms)
      (throw
       (ex-info
        (str "Undefined symbol(s) in subscription map for " subscriber-type
             " " subscriber-name "`. These symbols are used in subscription "
             "keypaths, but are not defined: " undefined-syms)
        (sym-map undefined-syms sub-map))))))

;;;;;;;;;;;;;;;;;;;; Platform detection ;;;;;;;;;;;;;;;;;;;;

(defn jvm? []
  #?(:clj true
     :cljs false))

(defn browser? []
  #?(:clj false
     :cljs (exists? js/navigator)))

(defn node? []
  #?(:clj false
     :cljs (boolean (= "nodejs" cljs.core/*target*))))

(defn platform-kw []
  (cond
    (jvm?) :jvm
    (node?) :node
    (browser?) :browser
    :else :unknown))
