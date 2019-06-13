(ns com.dept24c.vivo.utils
  (:require
   #?(:cljs [cljs.reader :as reader])
   #?(:clj [clojure.edn :as edn])
   #?(:cljs [clojure.pprint :as pprint])
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.client :as cc]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.lancaster.bilt :as bilt]
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

;;;;;;;;;;;;;;;;;;;; Protocols ;;;;;;;;;;;;;;;;;;;;

(defprotocol IBristleconeClient
  (<commit!
    [this branch db-id]
    [this branch db-id msg])
  (<create-branch [this source-branch target-branch])
  (<delete-branch [this branch])
  (<get [this db-id path])
  (<get-branches [this])
  (<get-head [this branch])
  (<get-key-count [this db-id path])
  (<get-keys
    [this db-id path]
    [this db-id path limit]
    [this db-id path limit offset])
  (<get-log-count [this branch])
  (<get-log
    [this branch]
    [this branch limit]
    [this branch limit offset])
  (<merge [this source-branch target-branch])
  (<transact!
    [this branch update-commands]
    [this branch update-commands msg])
  (<update! [this db-id update-commands]))

(defprotocol IBristleconeClientInternals
  (<fp-bytes->schema [this fp-bytes])
  (<read-block* [this block-num])
  (<schema->fp-bytes [this sch])
  (<write-block* [this block-num data]))

(defprotocol IBristleconeStorage
  (<allocate-block-num [this])
  (<read-block [this block-id])
  (<write-block [this block-id data]))

;;;;;;;;;;;;;;;;;;;; Schemas ;;;;;;;;;;;;;;;;;;;;

(def db-id-schema l/string-schema)
(def subject-id-schema l/string-schema)
(def valid-ops
  #{:set :remove :insert-before :insert-after
    :plus :minus :multiply :divide :mod})

(def op-schema (l/enum-schema :com.dept24c.vivo.utils/op
                              {:key-ns-type :none} (seq valid-ops)))

(l/def-union-schema path-item-schema
  bilt/keyword-schema
  l/string-schema
  l/int-schema)

(l/def-array-schema path-schema
  path-item-schema)

(defn long->non-neg-str [l]
  #?(:cljs (if (.isNegative l)
             (str "1" (.toString (.negate l)))
             (str "0" (.toString l)))
     :clj (if (neg? l)
            (str "1" (Long/toString (* -1 l) 10))
            (str "0" (Long/toString l 10)))))

(defn schema->value-rec-name [value-schema]
  (str "v-" (long->non-neg-str (l/fingerprint64 value-schema))))

(defn make-value-rec-schema [value-schema]
  (l/record-schema (keyword "com.dept24c.vivo.utils"
                            (schema->value-rec-name value-schema))
                   [[:v (l/maybe value-schema)]]))

(defn make-values-union-schema [state-schema]
  (l/union-schema (map make-value-rec-schema (l/sub-schemas state-schema))))

(defn make-update-cmd-schema [state-schema]
  (l/record-schema ::update-cmd
                   {:key-ns-type :none}
                   [[:path path-schema]
                    [:op op-schema]
                    [:arg (l/maybe (make-values-union-schema state-schema))]]))

(defn make-update-state-arg-schema [state-schema]
  (let [update-cmd-schema (make-update-cmd-schema state-schema)]
    (l/record-schema ::update-state-arg
                     {:key-ns-type :none}
                     [[:tx-info-str (l/maybe l/string-schema)]
                      [:update-cmds (l/array-schema update-cmd-schema)]])))

(l/def-record-schema get-state-arg-schema
  {:key-ns-type :none}
  [:db-id (l/maybe db-id-schema)]
  [:path path-schema])

(l/def-record-schema store-change-schema
  {:key-ns-type :none}
  [:db-id db-id-schema]
  [:tx-info-str l/string-schema])

(l/def-record-schema connect-store-arg-schema
  {:key-ns-type :none}
  [:store-name l/string-schema]
  [:branch l/string-schema])

(l/def-record-schema log-in-arg-schema
  {:key-ns-type :none}
  [:identifier l/string-schema]
  [:secret l/string-schema])

(defn make-sm-server-protocol [state-schema]
  (let [values-union-schema (make-values-union-schema state-schema)
        get-state-ret-schema (l/record-schema
                              ::get-state-ret-schema
                              {:key-ns-type :none}
                              [[:db-id (l/maybe db-id-schema)]
                               [:is-unauthorized (l/maybe l/boolean-schema)]
                               [:value (l/maybe values-union-schema)]])]
    {:roles [:state-manager :server]
     :msgs {:connect-store {:arg connect-store-arg-schema
                            :ret l/boolean-schema
                            :sender :state-manager}
            :get-state {:arg get-state-arg-schema
                        :ret get-state-ret-schema
                        :sender :state-manager}
            :log-in {:arg log-in-arg-schema
                     :ret l/boolean-schema
                     :sender :state-manager}
            :log-out {:arg l/null-schema
                      :ret l/boolean-schema
                      :sender :state-manager}
            :store-changed {:arg store-change-schema
                            :sender :server}
            :subject-id-changed {:arg (l/maybe subject-id-schema)
                                 :sender :server}
            :update-state {:arg (make-update-state-arg-schema state-schema)
                           :ret l/boolean-schema
                           :sender :state-manager}}}))

(def schema-at-path (sr/memoize-sr l/schema-at-path 100))

(defn value-rec-key [state-schema path]
  (let [value-schema (schema-at-path state-schema path)
        rec-name (schema->value-rec-name value-schema)]
    (keyword rec-name "v")))

(defn edn->value-rec [state-schema path v]
  (let [k (value-rec-key state-schema path)]
    {k v}))

(defn value-rec->edn [state-schema path value-rec]
  (get value-rec (value-rec-key state-schema path)))

(def b62alphabet
  [\A \B \C \D \E \F \G \H \I \J \K \L \M \N \O \P \Q \R \S \T \U \V \W \X \Y \Z
   \a \b \c \d \e \f \g \h \i \j \k \l \m \n \o \p \q \r \s \t \u \v \w \x \y \z
   \0 \1 \2 \3 \4 \5 \6 \7 \8 \9])

(defn long->b62 [l]
  (loop [i 0
         n l
         s ""]
    (let [c (b62alphabet (mod n 62))
          new-i (inc i)
          new-n (quot n 62)
          new-s (str s c)]
      (if (= 10 new-i)
        new-s
        (recur new-i new-n new-s)))))

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
                        (if (#{:vivo/tx-info} v)
                          acc
                          (throw (ex-info
                                  (str "Bad path. Must be a sequence or "
                                       ":vivo/tx-info.")
                                  (sym-map sym v sub-map))))))
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
