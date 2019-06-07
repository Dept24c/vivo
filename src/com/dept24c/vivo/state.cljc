(ns com.dept24c.vivo.state
  (:require
   [clojure.core.async :as ca]
   [clojure.set :as set]
   [clojure.string :as str]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.client :as cc]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.stockroom :as sr]
   [weavejester.dependency :as dep])
  #?(:clj
     (:import (clojure.lang ExceptionInfo))))

(def update-state-timeout-ms 5000)

(def default-sm-opts
  {:initial-local-state nil
   :initial-sys-state nil
   :log-error println
   :log-info println
   :sys-state-cache-size 1000})

(defprotocol IState
  (update-state! [this update-cmds tx-info cb])
  (subscribe! [this sub-id sub-map update-fn])
  (unsubscribe! [this sub-id])
  (handle-store-changed [this arg])
  (<get-in-sys-state [this db-id path]))

(defn throw-bad-path-root [path]
  (let [[head & tail] path
        disp-head (or head "nil")]
    (throw (ex-info (str "Paths must begin with either :local or :sys. Got `"
                         disp-head "` in path `" path "`.")
                    (u/sym-map path head)))))

(defn throw-bad-path-key [path k]
  (let [disp-k (or k "nil")]
    (throw (ex-info
            (str "Illegal key `" disp-k "` in path `" path "`. Only integers, "
                 "keywords, symbols, and strings are valid path keys.")
            (u/sym-map k path)))))

(defn split-updates [update-cmds]
  (reduce (fn [acc cmd]
            (let [{:keys [path op]} cmd
                  _ (when-not (sequential? path)
                      (throw (ex-info
                              (str "The `path` parameter of the update "
                                   "command must be a sequence. Got: `"
                                   path "`.")
                              (u/sym-map cmd path))))
                  [head & tail] path
                  _ (when-not (u/valid-ops op)
                      (throw (ex-info
                              (str "The `op` parameter of the update command "
                                   "is not a valid op. Got: `" op "`.")
                              (u/sym-map cmd op))))
                  k (case head
                      :local :local-cmds
                      :sys :sys-cmds
                      (throw-bad-path-root path))]
              (update acc k conj (assoc cmd :path tail))))
          {:local-cmds []
           :sys-cmds []}
          update-cmds))

(defn normalize-neg-k
  "Return the normalized key and the associated value or nil if key does not
   exist in value."
  [k v]
  (if (map? v)
    [k (v k)]
    (let [len (count v)
          norm-k (+ len k)]
      [norm-k (when (and (pos? len) (nat-int? norm-k) (< norm-k len))
                (v norm-k))])))

(defn get-in-state
  "Custom get-in fn that checks types and normalizes negative keys.
   Returns a map with :norm-path and :val keys."
  [state path]
  (reduce (fn [{:keys [norm-path val] :as acc} k]
            (let [[k* val*] (cond
                              (or (keyword? k) (nat-int? k) (string? k))
                              [k (when val
                                   (val k))]

                              (and (int? k) (neg? k))
                              (normalize-neg-k k val)

                              :else
                              (throw-bad-path-key path k))]
              (-> acc
                  (update :norm-path conj k*)
                  (assoc :val val*))))
          {:norm-path []
           :val state}
          path))

(defmulti eval-cmd (fn [state {:keys [op]}]
                     op))

(defmethod eval-cmd :set
  [state {:keys [path op arg]}]
  (let [{:keys [norm-path]} (get-in-state state path)]
    (if (seq norm-path)
      (assoc-in state norm-path arg)
      arg)))

(defmethod eval-cmd :remove
  [state {:keys [path]}]
  (let [parent-path (butlast path)
        k (last path)
        {:keys [norm-path val]} (get-in-state state parent-path)
        new-parent (if (map? val)
                     (dissoc val k)
                     (let [norm-i (if (nat-int? k)
                                    k
                                    (+ (count val) k))
                           [h t] (split-at norm-i val)]
                       (if (nat-int? norm-i)
                         (vec (concat h (rest t)))
                         val)))]
    (if (empty? norm-path)
      new-parent
      (assoc-in state norm-path new-parent))))

(defn insert* [state path op arg]
  (let [parent-path (butlast path)
        i (last path)
        _ (when-not (int? i)
            (throw (ex-info
                    (str "In " op " update expressions, the last element "
                         "of the path must be an integer, e.g. [:x :y -1] "
                         " or [:a :b :c 12]. Got: `" i "`.")
                    (u/sym-map parent-path i path op arg))))
        {:keys [norm-path val]} (get-in-state state parent-path)
        _ (when-not (or (vector? val) (nil? val))
            (throw (ex-info (str "Bad path in " op ". Path `" path "` does not "
                                 "point to a vector. Got: `" val "`.")
                            (u/sym-map op path val norm-path))))
        norm-i (if (nat-int? i)
                 i
                 (+ (count val) i))
        split-i (if (= :insert-before op)
                  norm-i
                  (inc norm-i))
        [h t] (split-at split-i val)
        new-t (cons arg t)
        new-parent (vec (concat h new-t))]
    (if (empty? norm-path)
      new-parent
      (assoc-in state norm-path new-parent))))

(defmethod eval-cmd :insert-before
  [state {:keys [path op arg]}]
  (insert* state path op arg))

(defmethod eval-cmd :insert-after
  [state {:keys [path op arg]}]
  (insert* state path op arg))

(defn eval-math-cmd [state cmd op-fn]
  (let [{:keys [path op arg]} cmd
        {:keys [norm-path val]} (get-in-state state path)
        _ (when-not (number? val)
            (throw (ex-info (str "Can't do math on non-numeric type. "
                                 "Value in state at path `"
                                 path "` is not a number. Got: " val ".")
                            (u/sym-map path cmd))))
        _ (when-not (number? arg)
            (throw (ex-info (str "Can't do math on non-numeric type. "
                                 "Arg `" arg "` in update command `"
                                 cmd "` is not a number.")
                            (u/sym-map path cmd op))))
        new-val (op-fn val arg)]
    (assoc-in state norm-path new-val)))

(defmethod eval-cmd :+
  [state cmd]
  (eval-math-cmd state cmd +))

(defmethod eval-cmd :-
  [state cmd]
  (eval-math-cmd state cmd -))

(defmethod eval-cmd :*
  [state cmd]
  (eval-math-cmd state cmd *))

(defmethod eval-cmd :/
  [state cmd]
  (eval-math-cmd state cmd /))

(defmethod eval-cmd :mod
  [state cmd]
  (eval-math-cmd state cmd mod))

(defn <make-df
  [local-state db-id ordered-sym-path-pairs tx-info-sym tx-info
   <get-in-sys-state]
  (au/go
    (try
      (let [last-i (dec (count ordered-sym-path-pairs))]
        (loop [df (if tx-info-sym
                    {tx-info-sym tx-info}
                    {})
               i 0]
          (let [[sym path] (nth ordered-sym-path-pairs i)
                [head & tail] (mapv #(if-not (symbol? %)
                                       %
                                       (or (df %)
                                           (throw
                                            (ex-info "Path value not found."
                                                     {:v %
                                                      :type :no-path-val}))))
                                    path)
                v (case head
                    :local (:val (get-in-state local-state tail))
                    :sys (au/<? (<get-in-sys-state db-id tail)))
                new-df (assoc df sym v)]
            (if (= last-i i)
              new-df
              (recur new-df (inc i))))))
      (catch #?(:clj Exception :cljs js/Error) e
        ;; Return false if path formation fails (happens on initial subscription
        ;; with incomplete state).
        (if (= :no-path-val (:type (ex-data e)))
          false
          (throw e))))))

(defn <notify-sub [local-state db-id tx-info log-error <get-in-sys-state sub]
  (ca/go
    (try
      (let [{:keys [update-fn ordered-sym-path-pairs tx-info-sym *last-df]} sub
            new-df (au/<? (<make-df local-state db-id ordered-sym-path-pairs
                                    tx-info-sym tx-info <get-in-sys-state))]
        (when (and new-df (not= @*last-df new-df))
          (reset! *last-df new-df)
          (update-fn new-df)))
      (catch #?(:clj Exception :cljs js/Error) e
        (log-error (str "Exception in <notify-sub: "
                        (u/ex-msg-and-stacktrace e)))))))

(defn <do-sys-updates
  [capsule-client log-info tx-info sys-state-schema sys-cmds]
  (au/go
    (try
      (let [update-cmds (reduce
                         (fn [acc {:keys [path op arg]}]
                           (conj acc {:path path
                                      :op op
                                      :arg (u/edn->value-rec sys-state-schema
                                                             path arg)}))
                         [] sys-cmds)
            us-arg {:tx-info-str (u/edn->str tx-info)
                    :update-cmds update-cmds}
            ret (au/<? (cc/<send-msg capsule-client :update-state us-arg
                                     update-state-timeout-ms))]
        (:db-id ret))
      (catch #?(:clj Exception :cljs js/Error) e
        (if-not (str/includes? (u/ex-msg e) "timed out")
          (throw e)
          (do
            (log-info (str ":update-state call timed out after "
                           update-state-timeout-ms " ms."))
            nil))))))

(defn check-path [path sub-syms sub-map]
  (reduce (fn [acc k]
            (when (and (symbol? k) (not (sub-syms k)))
              (throw (ex-info
                      (str "Path symbol `" k "` in path `" path
                           "` is not defined as a key in the subscription map.")
                      (u/sym-map path k sub-map))))
            (if-not (or (keyword? k) (int? k) (string? k) (symbol? k))
              (throw-bad-path-key path k)
              (conj acc k)))
          [] path))

(defn make-sub-info [sub-map]
  (let [sub-syms (set (keys sub-map))
        info (reduce-kv
              (fn [acc sym v]
                (when-not (symbol? sym)
                  (throw (ex-info
                          (str "All keys in sub-map must be symbols. Got `"
                               sym "`.")
                          (u/sym-map sym sub-map))))
                (if (= :vivo/tx-info v)
                  (assoc acc :tx-info-sym sym)
                  (let [path v
                        [head & tail] (check-path path sub-syms sub-map)
                        deps (filter symbol? path)]
                    (when-not (#{:local :sys} head)
                      (throw-bad-path-root path))
                    (cond-> (update acc :sym->path assoc sym path)
                      (seq deps) (update :g #(reduce (fn [g dep]
                                                       (dep/depend g sym dep))
                                                     % deps))))))
              {:tx-info-sym nil
               :g (dep/graph)
               :sym->path {}}
              sub-map)
        {:keys [tx-info-sym g sym->path]} info
        ordered-dep-syms (dep/topo-sort g)
        no-dep-syms (set/difference (set (keys sym->path))
                                    (set ordered-dep-syms))]
    {:ordered-sym-path-pairs (reduce (fn [acc sym]
                                       (let [path (sym->path sym)]
                                         (conj acc [sym path])))
                                     []
                                     (concat (seq no-dep-syms)
                                             ordered-dep-syms))
     :tx-info-sym tx-info-sym}))

(defn no-server-exception []
  (ex-info (str "Can't update :sys state because the `get-server-url` option "
                "was not provided to the state-manager constructor.")
           {:reason :no-get-server-url}))

(defrecord StateManager [capsule-client sys-state-schema log-info log-error
                         sys-state-cache *local-state *sub-id->sub]
  IState
  (<get-in-sys-state [this db-id* path]
    (au/go
      (or (and db-id* (sr/get sys-state-cache [db-id* path]))
          (let [arg {:db-id db-id*
                     :path path}
                ret (au/<? (cc/<send-msg capsule-client :get-state arg))
                {:keys [db-id value]} ret
                v (u/value-rec->edn sys-state-schema path value)]
            (sr/put sys-state-cache [db-id path] v)
            v))))

  (update-state! [this update-cmds tx-info cb]
    (when-not (sequential? update-cmds)
      (throw (ex-info "The update-cmds parameter must be a sequence."
                      (u/sym-map update-cmds))))
    (let [{:keys [local-cmds sys-cmds]} (split-updates update-cmds)]
      (ca/go
        (try
          ;; db-id will be nil when there are no sys updates or if
          ;; <do-sys-updates times out
          (let [db-id (when (seq sys-cmds)
                        (au/<? (<do-sys-updates
                                capsule-client log-info tx-info
                                sys-state-schema sys-cmds)))
                local-state (if (seq local-cmds)
                              (swap! *local-state
                                     #(reduce eval-cmd % local-cmds))
                              @*local-state)]
            (doseq [sub (vals @*sub-id->sub)]
              (<notify-sub local-state db-id tx-info log-error
                           (partial <get-in-sys-state this) sub))
            (when cb
              (cb true)))
          (catch #?(:clj Exception :cljs js/Error) e
            (log-error (str "Exception in update-state!: "
                            (u/ex-msg-and-stacktrace e)))
            (when cb
              (cb e))))))
    nil)

  (handle-store-changed [this arg]
    (let [{:keys [db-id tx-info-str]} arg
          local-state @*local-state
          tx-info (u/str->edn tx-info-str)]
      (doseq [sub (vals @*sub-id->sub)]
        (<notify-sub local-state db-id tx-info log-error
                     (partial <get-in-sys-state this) sub))))

  (subscribe! [this sub-id sub-map update-fn]
    (when-not (string? sub-id)
      (throw (ex-info "The sub-id parameter must be a string."
                      (u/sym-map sub-id))))
    (when-not (map? sub-map)
      (throw (ex-info "The sub-map parameter must be a map."
                      (u/sym-map sub-map))))
    (when-not (pos? (count sub-map))
      (throw (ex-info "The sub-map parameter must contain at least one entry."
                      (u/sym-map sub-map))))
    (when-not (ifn? update-fn)
      (throw (ex-info "The update-fn parameter must be a function."
                      (u/sym-map update-fn))))
    (let [{:keys [ordered-sym-path-pairs tx-info-sym]} (make-sub-info sub-map)]
      (ca/go
        (try
          (let [tx-info :initial-subscription
                df (au/<? (<make-df @*local-state nil ordered-sym-path-pairs
                                    tx-info-sym tx-info
                                    (partial <get-in-sys-state this)))
                *last-df (atom df)
                sub (u/sym-map update-fn ordered-sym-path-pairs
                               tx-info-sym *last-df)]
            (swap! *sub-id->sub assoc sub-id sub)
            (when df
              (update-fn df)))
          (catch #?(:clj Exception :cljs js/Error) e
            (log-error (str "Exception in subscribe!: "
                            (u/ex-msg-and-stacktrace e)))))))
    nil)

  (unsubscribe! [this sub-id]
    (when-not (string? sub-id)
      (throw (ex-info "The sub-id parameter must be a string."
                      (u/sym-map sub-id))))
    (swap! *sub-id->sub dissoc sub-id)
    nil))

(defn make-capsule-client
  [get-server-url sys-state-schema sys-state-store-name sys-state-store-branch]
  (when-not sys-state-schema
    (throw (ex-info (str "Missing `:sys-state-schema` option in state-manager "
                         "constructor.")
                    {})))
  (when-not sys-state-store-branch
    (throw (ex-info (str "Missing `:sys-state-store-branch` option in "
                         "state-manager constructor.")
                    {})))
  (let [protocol (u/make-sm-server-protocol sys-state-schema)
        get-credentials (constantly {:subject-id "state-manager"
                                     :subject-secret ""})
        client (cc/client get-server-url get-credentials
                          protocol :state-manager)]
    (cc/send-msg client :connect-store {:store-name sys-state-store-name
                                        :branch sys-state-store-branch})
    client))

(defn state-manager [opts]
  (let [opts* (merge default-sm-opts opts)
        {:keys [initial-local-state get-server-url log-error
                log-info sys-state-cache-size sys-state-schema
                sys-state-store-name sys-state-store-branch]} opts*
        capsule-client (when get-server-url
                         (make-capsule-client get-server-url sys-state-schema
                                              sys-state-store-name
                                              sys-state-store-branch))
        *local-state (atom initial-local-state)
        *sub-id->sub (atom {})
        sys-state-cache (sr/stockroom sys-state-cache-size)
        sm (->StateManager capsule-client sys-state-schema log-info log-error
                           sys-state-cache *local-state *sub-id->sub)]
    (when get-server-url
      (cc/set-handler capsule-client :store-changed
                      (partial handle-store-changed sm)))
    sm))
