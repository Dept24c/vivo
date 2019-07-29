(ns com.dept24c.vivo.utils
  (:require
   #?(:cljs [cljs.reader :as reader])
   #?(:clj [clojure.edn :as edn])
   #?(:cljs [clojure.pprint :as pprint])
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.stockroom :as sr]
   #?(:clj [puget.printer :refer [cprint cprint-str]]))
  #?(:cljs
     (:require-macros
      [com.dept24c.vivo.utils :refer [sym-map]])))

(defmacro sym-map
  "Builds a map from symbols.
   Symbol names are turned into keywords and become the map's keys.
   Symbol values become the map's values.
  (let [a 1
        b 2]
    (sym-map a b))  =>  {:a 1 :b 2}"
  [& syms]
  (zipmap (map keyword syms) syms))

(defn pprint [x]
  #?(:clj (.write *out* (cprint-str x))
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

;;;;;;;;;;;;;;;;;;;; Schemas ;;;;;;;;;;;;;;;;;;;;

(def db-id-schema l/string-schema)
(def subject-id-schema l/string-schema)
(def valid-ops  [:set :remove :insert-before :insert-after
                 :plus :minus :multiply :divide :mod])
(def op-schema (l/enum-schema :com.dept24c.vivo.utils/op
                              valid-ops))
(def token-schema l/string-schema)

(l/def-union-schema path-item-schema
  l/keyword-schema
  l/string-schema
  l/int-schema)

(l/def-array-schema path-schema
  path-item-schema)

(defn long->non-neg-str [^long l]
  #?(:cljs (if (.isNegative l)
             (str "1" (.toString (.negate l)))
             (str "0" (.toString l)))
     :clj (if (neg? l)
            (str "1" (Long/toString (* -1 l) 10))
            (str "0" (Long/toString l 10)))))

(defn schema->value-rec-name [value-schema]
  (str "v-" (long->non-neg-str (l/fingerprint64 value-schema))))

(defn make-value-rec-schema [value-schema]
  (let [vr-name (schema->value-rec-name value-schema)]
    (l/record-schema (keyword "com.dept24c.vivo.utils" vr-name)
                     [[(keyword vr-name) (l/maybe value-schema)]])))

(defn make-values-union-schema [state-schema]
  (l/union-schema (mapv make-value-rec-schema (l/sub-schemas state-schema))))

(defn make-update-cmd-schema [state-schema]
  (l/record-schema ::update-cmd
                   [[:path (l/maybe path-schema)]
                    [:op op-schema]
                    [:arg (l/maybe (make-values-union-schema state-schema))]]))

(defn make-update-state-arg-schema [state-schema]
  (let [update-cmd-schema (make-update-cmd-schema state-schema)]
    (l/record-schema ::update-state-arg
                     [[:update-cmds (l/array-schema update-cmd-schema)]])))

(l/def-record-schema get-state-arg-schema
  [:db-id db-id-schema]
  [:path path-schema])

(l/def-record-schema sys-state-change-schema
  [:cur-db-id db-id-schema]
  [:prev-db-id db-id-schema]
  [:updated-paths (l/array-schema (l/maybe path-schema))])

(l/def-record-schema log-in-arg-schema
  [:identifier l/string-schema]
  [:secret l/string-schema])

(l/def-record-schema log-in-ret-schema
  [:subject-id subject-id-schema]
  [:token token-schema])

(l/def-record-schema branch-state-source-schema
  [:branch l/string-schema])

(l/def-record-schema temp-branch-state-source-schema
  [:temp-branch/db-id (l/maybe db-id-schema)])

(l/def-record-schema token-info-schema
  [:expiration-time-mins l/long-schema]
  [:subject-id subject-id-schema])

(l/def-record-schema subject-info-schema
  [:hashed-secret l/string-schema]
  [:identifiers (l/array-schema l/string-schema)])

(def token-map-schema (l/map-schema token-info-schema))

(def state-source-schema (l/union-schema [branch-state-source-schema
                                          temp-branch-state-source-schema]))

(defn make-get-state-ret-schema [values-union-schema]
  (l/record-schema ::get-state-ret
                   [[:vivo/unauthorized (l/maybe l/boolean-schema)]
                    [:v (l/maybe values-union-schema)]]))

(defn make-sm-server-protocol [state-schema]
  (let [values-union-schema (make-values-union-schema state-schema)]
    {:roles [:state-manager :server]
     :msgs {:get-state {:arg get-state-arg-schema
                        :ret (make-get-state-ret-schema values-union-schema)
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
            :update-state {:arg (make-update-state-arg-schema state-schema)
                           :ret (l/union-schema [sys-state-change-schema
                                                 l/boolean-schema])
                           :sender :state-manager}}}))

(def schema-at-path (sr/memoize-sr l/schema-at-path 100))

(defn value-rec-key [state-schema path]
  (let [value-schema (schema-at-path state-schema path)
        vr-name (schema->value-rec-name value-schema)]
    (keyword vr-name)))

(defn edn->value-rec [state-schema path v]
  (let [k (value-rec-key state-schema path)]
    {k v}))

(defn value-rec->edn [value-rec]
  (-> value-rec first second))

(defn get-undefined-syms [sub-map]
  (let [defined-syms (set (keys sub-map))]
    (vec (reduce-kv (fn [acc sym v]
                      (if (sequential? v)
                        (reduce (fn [acc* k]
                                  (if-not (symbol? k)
                                    acc*
                                    (if (defined-syms k)
                                      acc*
                                      (conj acc* k))))
                                acc v)
                        (throw (ex-info
                                (str "Bad path. Must be a sequence.")
                                (sym-map sym v sub-map)))))
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
