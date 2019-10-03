(ns com.dept24c.vivo.utils
  (:require
   #?(:cljs [cljs.reader :as reader])
   #?(:clj [clojure.edn :as edn])
   #?(:cljs [clojure.pprint :as pprint])
   [clojure.set :as set]
   [clojure.string :as str]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.logging :as logging]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.stockroom :as sr]
   #?(:clj [puget.printer :refer [cprint cprint-str]])
   [weavejester.dependency :as dep])
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

(defprotocol ISchemaStore
  (<schema->fp [this schema])
  (<fp->schema [this fp]))

(defprotocol IVivoClient
  (<add-subject!
    [this identifier secret]
    [this identifier secret subject-id])
  (<deserialize-value [this path ret])
  (<get-in-sys-state [this db-id path])
  (<handle-updates [this updates cb])
  (<make-state-info
    [this sub-map-or-ordered-pairs subscriber-name resolution-map]
    [this sub-map-or-ordered-pairs subscriber-name resolution-map
     local-state db-id])
  (<ssr [this component-fn component-name static-markup?])
  (<update-sys-state [this update-commands])
  (<wait-for-conn-init [this])
  (get-local-state [this sub-map resolution-map component-name])
  (handle-sys-state-changed [this arg metadata])
  (log-in! [this identifier secret cb])
  (log-out! [this])
  (shutdown! [this])
  (ssr-get-state! [this sub-map resolution-map])
  (ssr? [this])
  (subscribe! [this sub-map cur-state update-fn subscriber-name resolution-map])
  (update-cmd->serializable-update-cmd [this cmds])
  (update-state! [this update-cmds cb]))

(defprotocol IDataStorage
  (<delete-reference! [this reference])
  (<get-in [this data-id schema path prefix])
  (<get-in-reference [this reference schema path prefix])
  (<update
    [this data-id schema update-commands prefix]
    [this data-id schema update-commands prefix tx-fns])
  (<update-reference!
    [this reference schema update-commands prefix]
    [this reference schema update-commands prefix tx-fns]))

(defprotocol IDataBlockStorage
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
   Relationship is one of :sibling, :parent, :child, or :equal.
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

(def path-item-sequence
  (l/array-schema path-item-schema))

(l/def-array-schema path-schema
  path-item-schema)

(l/def-union-schema augmented-path-item-schema
  l/keyword-schema
  l/string-schema
  l/long-schema
  path-item-sequence)

(l/def-array-schema augmented-path-schema
  augmented-path-item-schema)

(l/def-record-schema serialized-value-schema
  [:fp :required fp-schema]
  [:bytes :required l/bytes-schema])

(l/def-record-schema serializable-update-command-schema
  [:path path-schema]
  [:op op-schema]
  [:arg serialized-value-schema])

(def serializable-update-commands-schema
  (l/array-schema serializable-update-command-schema))

(def db-id-schema l/string-schema)

(l/def-record-schema get-state-arg-schema
  [:db-id db-id-schema]
  [:path augmented-path-schema])

(l/def-enum-schema unauthorized-schema
  :vivo/unauthorized)

(def get-state-ret-schema
  (l/union-schema [l/null-schema unauthorized-schema serialized-value-schema]))

(l/def-record-schema update-info-schema
  [:norm-path path-schema]
  [:op op-schema])

(l/def-record-schema sys-state-change-schema
  [:new-db-id db-id-schema]
  [:prev-db-id db-id-schema]
  [:update-infos (l/array-schema update-info-schema)])

(def update-state-ret-schema
  (l/union-schema [l/null-schema unauthorized-schema sys-state-change-schema]))

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
  [:temp-branch/db-id db-id-schema])

(def state-source-schema (l/union-schema [branch-state-source-schema
                                          temp-branch-state-source-schema]))

(l/def-record-schema create-branch-arg-schema
  [:branch branch-name-schema]
  [:db-id db-id-schema])

;;;;;;;;;;;;;;;;;;;; Protocols ;;;;;;;;;;;;;;;;;;;;

(def client-server-protocol
  {:roles [:client :server]
   :msgs {:add-subject {:arg add-subject-arg-schema
                        :ret subject-id-schema
                        :sender :client}
          :add-subject-identifier {:arg l/string-schema
                                   :ret l/boolean-schema
                                   :sender :client}
          :change-secret {:arg l/string-schema
                          :ret l/boolean-schema
                          :sender :client}
          :get-schema-pcf {:arg l/long-schema
                           :ret (l/maybe l/string-schema)
                           :sender :either}
          :store-schema-pcf {:arg l/string-schema
                             :ret l/boolean-schema
                             :sender :client}
          :get-state {:arg get-state-arg-schema
                      :ret get-state-ret-schema
                      :sender :client}
          :log-in {:arg log-in-arg-schema
                   :ret (l/maybe log-in-ret-schema)
                   :sender :client}
          :log-in-w-token {:arg token-schema
                           :ret (l/maybe subject-id-schema)
                           :sender :client}
          :log-out {:arg l/null-schema
                    :sender :client}
          :set-state-source {:arg state-source-schema
                             :ret db-id-schema
                             :sender :client}
          :sys-state-changed {:arg sys-state-change-schema
                              :sender :server}
          ;; We need to return the state change from :update-state so
          ;; sys+local updates remain atomic
          :update-state {:arg serializable-update-commands-schema
                         :ret update-state-ret-schema
                         :sender :client}}})

(def admin-client-server-protocol
  {:roles [:admin-client :server]
   :msgs {:create-branch {:arg create-branch-arg-schema
                          :ret l/boolean-schema
                          :sender :admin-client}
          :delete-branch {:arg branch-name-schema
                          :ret l/boolean-schema
                          :sender :admin-client}}})

;;;;;;;;;;;;;;;;;;;; Helper fns ;;;;;;;;;;;;;;;;;;;;

(defn check-sub-map
  [subscriber-name subscriber-type sub-map]
  (when-not (map? sub-map)
    (throw (ex-info "The sub-map parameter must be a map."
                    (sym-map sub-map))))
  (when-not (pos? (count sub-map))
    (throw (ex-info "The sub-map parameter must contain at least one entry."
                    (sym-map sub-map))))
  (doseq [[sym path] sub-map]
    (when-not (symbol? sym)
      (throw (ex-info (str "Bad key `" sym "` in subscription map. Keys must "
                           "be symbols.")
                      {:bad-key sym})))
    (cond
      (sequential? path)
      (let [[head & tail] path]
        (when (and (#{:subscriber :component} head)
                   (not (seq tail)))
          (throw (ex-info
                  (str "Missing " (name head) " id in " head " path. ")
                  (sym-map path)))))

      (not (#{:vivo/subject-id :vivo/component-id :vivo/subscriber-id} path))
      (throw (ex-info
              (str "Bad path. Paths must be either a sequence or  "
                   "one of the special :vivo keywords. (:vivo/subject-id, "
                   ":vivo/subscriber-id, or :vivo/component-id)")
              (sym-map sym path sub-map))))))

(defn local-or-vivo-only? [sub-map]
  (reduce (fn [acc path]
            (if (or
                 (= :vivo/subject-id path)
                 (= :local (first path)))
              acc
              (reduced false)))
          true (vals sub-map)))

(defn cartesian-product [colls]
  (if (empty? colls)
    '(())
    (for [more (cartesian-product (rest colls))
          x (first colls)]
      (cons x more))))

(defn parse-path [path]
  (let [info (reduce
              (fn [{:keys [template in-progress colls coll-index] :as acc} part]
                (if-not (sequential? part)
                  (update acc :in-progress conj part)
                  (cond-> acc
                    (seq in-progress) (update :template conj in-progress)
                    (seq in-progress) (assoc :in-progress [])
                    true (update :colls conj part)
                    true (update :template conj coll-index)
                    true (update :coll-index inc))))
              {:template []
               :in-progress []
               :colls []
               :coll-index 0}
              path)
        {:keys [in-progress]} info]
    (cond-> info
      (seq in-progress) (update :template conj in-progress)
      true (select-keys [:template :colls]))))

(defn expand-path [path]
  (let [{:keys [template colls]} (parse-path path)]
    (reduce (fn [acc combo]
              (conj acc (vec (reduce (fn [acc* part]
                                       (concat acc* (if (number? part)
                                                      [(nth combo part)]
                                                      part)))
                                     '() template))))
            [] (cartesian-product colls))))

(defn path->schema-path [path]
  (reduce (fn [acc item]
            (conj acc (if (sequential? item)
                        (first item)
                        item)))
          [] path))

(defn path->schema [path->schema-cache state-schema path]
  (let [sch-path (path->schema-path path)
        seq-path? (not= path sch-path)]
    (or (sr/get path->schema-cache path)
        (let [sch (l/schema-at-path state-schema sch-path)
              sch* (if seq-path?
                     (l/array-schema sch)
                     sch)]
          (sr/put! path->schema-cache path sch*)
          sch*))))

(defn get-non-numeric-part [path]
  (take-while #(not (number? %)) path))

(defn update-numeric? [updated-path sub-path op]
  ;; TODO: Improve this using op / normalized paths
  (let [u-front (get-non-numeric-part updated-path)
        s-front (get-non-numeric-part sub-path)]
    (if (or (not (seq u-front)) (not (seq s-front)))
      true
      (let [[relationship _] (relationship-info u-front s-front)]
        (not= :sibling relationship)))))

(defn update-sub?* [update-infos sub-path]
  (reduce (fn [acc {:keys [norm-path op] :as update-info}]
            (cond
              (= :vivo/subject-id sub-path)
              (if (= :vivo/subject-id norm-path)
                (reduced true)
                false)

              (= :vivo/subject-id norm-path)
              (if (= :vivo/subject-id sub-path)
                (reduced true)
                false)

              (#{:vivo/subscriber-id :vivo/component-id}  sub-path)
              false ;; subscriber never gets updated

              (or (some number? norm-path)
                  (some number? sub-path))
              (if (update-numeric? norm-path sub-path op)
                (reduced true)
                false)

              :else
              (let [[relationship _] (relationship-info
                                      (or norm-path [])
                                      (or sub-path []))]
                (if (= :sibling relationship)
                  false
                  ;; TODO: Compare values here if :parent
                  (reduced true)))))
          false update-infos))

(defn update-sub? [update-infos sub-paths]
  (reduce (fn [acc sub-path]
            (if (update-sub?* update-infos sub-path)
              (reduced true)
              false))
          false sub-paths))

(defn throw-missing-path-key
  ([k path sub-map]
   (throw-missing-path-key k path sub-map nil))
  ([k path sub-map subscriber-name]
   (throw (ex-info (str "Could not find a value for key `" k "` in path `"
                        path "` of " (if subscriber-name
                                       (str "subscriber `" subscriber-name)
                                       (str "sub-map `" sub-map))
                        "`.")
                   (sym-map k path sub-map subscriber-name)))))

(defn throw-bad-path-root [path]
  (let [[head & tail] path
        disp-head (or head "nil")]
    (throw (ex-info (str "Paths must begin with :local, :sys, :subscriber, or "
                         ":component. Got `" disp-head "` in path `" path "`.")
                    (sym-map path head)))))

(defn throw-bad-path-key [path k]
  (let [disp-k (or k "nil")]
    (throw (ex-info
            (str "Illegal key `" disp-k "` in path `" path "`. Only integers, "
                 "keywords, symbols, and strings are valid path keys.")
            (sym-map k path)))))

(defn resolve-symbols-in-path [acc ordered-pairs subscriber-name path]
  (when (sequential? path)
    (mapv (fn [k]
            (if (symbol? k)
              (or (get-in acc [:state k])
                  (throw-missing-path-key k path ordered-pairs subscriber-name))
              k))
          path)))

(defn check-path [path sub-map]
  (if (= :vivo/subject-id path)
    path
    (reduce (fn [acc k]
              (if-not (or (keyword? k) (int? k) (string? k) (symbol? k))
                (throw-bad-path-key path k)
                (conj acc k)))
            [] path)))

(defn sub-map->ordered-pairs [sub-map->op-cache sub-map]
  (or (sr/get sub-map->op-cache sub-map)
      (let [sub-syms (set (keys sub-map))
            info (reduce-kv
                  (fn [acc sym path]
                    (when-not (symbol? sym)
                      (throw (ex-info
                              (str "All keys in sub-map must be symbols. Got `"
                                   sym "`.")
                              (sym-map sym sub-map))))
                    (if (#{:vivo/subject-id
                           :vivo/subscriber-id
                           :vivo/component-id} path)
                      (update acc :sym->path assoc sym path)
                      (let [[head & tail] (check-path path sub-map)
                            deps (filter symbol? path)]
                        (when-not (#{:subscriber :component :local :sys} head)
                          (throw-bad-path-root path))
                        (cond-> (update acc :sym->path assoc sym path)
                          (seq deps) (update :g #(reduce
                                                  (fn [g dep]
                                                    (dep/depend g sym dep))
                                                  % deps))))))
                  {:g (dep/graph)
                   :sym->path {}}
                  sub-map)
            {:keys [g sym->path]} info
            ordered-dep-syms (dep/topo-sort g)
            no-dep-syms (set/difference (set (keys sym->path))
                                        (set ordered-dep-syms))
            pairs (reduce (fn [acc sym]
                            (if-let [path (sym->path sym)]
                              (conj acc [sym path])
                              acc))
                          []
                          (concat (seq no-dep-syms)
                                  ordered-dep-syms))]
        (sr/put! sub-map->op-cache sub-map pairs)
        pairs)))

(defn empty-sequence-in-path? [path]
  (reduce (fn [acc element]
            (if (and (sequential? element)
                     (not (seq element)))
              (reduced true)
              acc))
          false path))

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
