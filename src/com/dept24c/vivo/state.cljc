(ns com.dept24c.vivo.state
  (:require
   [clojure.core.async :as ca]
   [clojure.set :as set]
   [clojure.string :as str]
   [com.dept24c.bristlecone :as bc]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.client :as cc]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.stockroom :as sr]
   [weavejester.dependency :as dep]))

(def default-sm-opts
  {:log-error println
   :log-info println
   :state-cache-size 100})

(def get-state-timeout-ms 30000)
(def update-state-timeout-ms 30000)

(defprotocol IStateManager
  (<get-in-sys-state [this db-id path])
  (<handle-sys-and-local-updates [this sys-cmds local-cmds paths cb])
  (<handle-sys-state-changed [this arg metadata])
  (<handle-sys-updates-only [this sys-cmds paths cb])
  (<make-state-info [this ordered-pairs local-state db-id])
  (<update-sys-state [this update-commands])
  (<wait-for-conn-init [this])
  (handle-local-updates-only [this local-cmds paths cb])
  (log-in! [this identifier secret cb])
  (log-out! [this cb])
  (notify-subs [this updated-paths notify-all])
  (shutdown! [this])
  (start-update-loop [this])
  (subscribe! [this sub-map cur-state update-fn])
  (unsubscribe! [this sub-id])
  (update-state! [this update-cmds cb]))

(defn local-path? [path]
  (= :local (first path)))

(defn sys-path? [path]
  (= :sys (first path)))

(defn throw-bad-path-root [path]
  (let [[head & tail] path
        disp-head (or head "nil")]
    (throw (ex-info (str "Paths must begin with either :local, :conn, or :sys. "
                         "Got `" disp-head "` in path `" path "`.")
                    (u/sym-map path head)))))

(defn throw-bad-path-key [path k]
  (let [disp-k (or k "nil")]
    (throw (ex-info
            (str "Illegal key `" disp-k "` in path `" path "`. Only integers, "
                 "keywords, symbols, and strings are valid path keys.")
            (u/sym-map k path)))))

(defn make-update-info [update-cmds]
  (reduce (fn [acc cmd]
            (let [{:keys [path op]} cmd
                  _ (when-not (sequential? path)
                      (throw (ex-info
                              (str "The `path` parameter of the update "
                                   "command must be a sequence. Got: `"
                                   path "`.")
                              (u/sym-map cmd path))))
                  [head & tail] path
                  _ (when-not ((set u/valid-ops) op)
                      (throw (ex-info
                              (str "The `op` parameter of the update command "
                                   "is not a valid op. Got: `" op "`.")
                              (u/sym-map cmd op))))
                  k (case head
                      :local :local-cmds
                      :sys :sys-cmds
                      (throw-bad-path-root path))]
              (-> acc
                  (update k conj (assoc cmd :path tail))
                  (update :paths conj path))))
          {:local-cmds []
           :sys-cmds []
           :paths []}
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

(defn sub-map->ordered-pairs [sm->op-cache sub-map]
  (or (sr/get sm->op-cache sub-map)
      (let [sub-syms (set (keys sub-map))
            info (reduce-kv
                  (fn [acc sym path]
                    (when-not (symbol? sym)
                      (throw (ex-info
                              (str "All keys in sub-map must be symbols. Got `"
                                   sym "`.")
                              (u/sym-map sym sub-map))))
                    (let [[head & tail] (check-path path sub-syms sub-map)
                          deps (filter symbol? path)]
                      (when-not (#{:local :conn :sys} head)
                        (throw-bad-path-root path))
                      (cond-> (update acc :sym->path assoc sym path)
                        (seq deps) (update :g #(reduce (fn [g dep]
                                                         (dep/depend g sym dep))
                                                       % deps)))))
                  {:g (dep/graph)
                   :sym->path {}}
                  sub-map)
            {:keys [g sym->path]} info
            ordered-dep-syms (dep/topo-sort g)
            no-dep-syms (set/difference (set (keys sym->path))
                                        (set ordered-dep-syms))
            pairs (reduce (fn [acc sym]
                            (let [path (sym->path sym)]
                              (conj acc [sym path])))
                          []
                          (concat (seq no-dep-syms)
                                  ordered-dep-syms))]
        (sr/put sm->op-cache sub-map pairs)
        pairs)))

(defn get-sub-id [*last-sub-id]
  (swap! *last-sub-id (fn [sub-id]
                        (let [new-sub-id (inc sub-id)]
                          (if (> new-sub-id 1e9)
                            0
                            new-sub-id)))))

(defn update-sub?* [updated-paths sub-path]
  (reduce (fn [acc updated-path]
            (if (some number? updated-path) ;; Numeric paths are complex...
              (reduced true)
              (let [[relationship _] (u/relationship-info
                                      (or updated-path [])
                                      (or sub-path []))]
                (if (= :sibling relationship)
                  false
                  (reduced true)))))
          false updated-paths))

(defn update-sub? [updated-paths sub-paths]
  (reduce (fn [acc sub-path]
            (if (or (some number? sub-path) ;; Numeric paths are complex...
                    (update-sub?* updated-paths sub-path))
              (reduced true)
              false))
          false sub-paths))

(defrecord StateManager [capsule-client sys-state-schema sys-state-source
                         log-info log-error state-cache sm->op-cache
                         update-ch *local-state *sub-id->sub *cur-db-id
                         *last-sub-id *conn-initialized? *stopped?]
  IStateManager
  (log-in! [this identifier secret cb]
    ;; (au/<? (<wait-for-conn-init this))
    )
  (log-out! [this cb]
    ;;(au/<? (<wait-for-conn-init this))
    )

  (shutdown! [this]
    (reset! *stopped? true)
    (cc/shutdown capsule-client)
    (log-info "State manager stopped."))

  (<make-state-info
    [this ordered-pairs local-state db-id]
    (au/go
      (let [init {:state {}
                  :paths []}]
        (if-not (seq ordered-pairs)
          init
          (loop [acc init
                 i 0]
            (let [[sym path] (nth ordered-pairs i)
                  resolved-path (mapv (fn [k]
                                        (if (symbol? k)
                                          (get-in acc [:state k])
                                          k))
                                      path)
                  v (cond
                      (local-path? path)
                      (->> (rest resolved-path)
                           (get-in-state local-state)
                           (:val))

                      (sys-path? path)
                      ;; TODO: Optimize by getting multiple paths in one call
                      (au/<? (<get-in-sys-state this db-id
                                                (rest resolved-path))))
                  new-acc (-> acc
                              (assoc-in [:state sym] v)
                              (update :paths conj resolved-path))]
              (if (= (dec (count ordered-pairs)) i)
                new-acc
                (recur new-acc (inc i)))))))))

  (subscribe! [this sub-map cur-state update-fn*]
    (let [sub-id (get-sub-id *last-sub-id)
          opairs (sub-map->ordered-pairs sm->op-cache sub-map)
          <make-si (partial <make-state-info this opairs)]
      (u/check-sub-map sub-id "subscriber" sub-map)
      (ca/go
        (try
          (au/<? (<wait-for-conn-init this))
          (let [local-state @*local-state
                {:keys [paths state]} (au/<? (<make-si local-state
                                                       @*cur-db-id))
                update-fn (fn [local-state db-id]
                            (ca/go
                              (try
                                (let [si (au/<? (<make-si local-state db-id))
                                      {uf-state :state} si]
                                  (when-not (= cur-state uf-state)
                                    (update-fn* uf-state)))
                                (catch #?(:cljs js/Error :clj Throwable) e
                                  (log-error
                                   (str "Exception calling update-fn:\n"
                                        (u/ex-msg-and-stacktrace e)))))))
                sub (u/sym-map paths update-fn)]
            (swap! *sub-id->sub assoc sub-id sub)
            (when-not (= cur-state state)
              (update-fn* state)))
          (catch #?(:cljs js/Error :clj Exception) e
            (log-error (str "Error in subscribe!\n"
                            (u/ex-msg-and-stacktrace e))))))
      sub-id))

  (unsubscribe! [this sub-id]
    (swap! *sub-id->sub dissoc sub-id)
    nil)

  (notify-subs [this updated-paths notify-all?]
    (let [local-state @*local-state
          db-id @*cur-db-id]
      (doseq [[sub-id sub] @*sub-id->sub]
        (let [{:keys [paths update-fn]} sub]
          (when (or notify-all?
                    (update-sub? updated-paths paths))
            (update-fn local-state db-id))))))

  (<wait-for-conn-init [this]
    (au/go
      (loop [tries-remaining 300]
        (when (zero? tries-remaining)
          (throw (ex-info "Timed out waiting for connection to initialize."
                          {:cause :init-timeout})))
        (cond
          @*conn-initialized? true
          @*stopped? false
          :else (do
                  (ca/<! (ca/timeout 200))
                  (recur (dec tries-remaining)))))))

  (<handle-sys-updates-only [this sys-cmds paths cb]
    (au/go
      (let [ret (au/<? (<update-sys-state this sys-cmds))
            cb* (or cb (constantly nil))]
        (if-not ret
          (cb* false)
          (let [{:keys [prev-db-id cur-db-id updated-paths]} ret
                local-db-id @*cur-db-id
                notify-all? (not= prev-db-id local-db-id)]
            (if (or (nil? local-db-id)
                    (bc/earlier? local-db-id cur-db-id))
              (do
                (reset! *cur-db-id cur-db-id)
                (notify-subs this updated-paths notify-all?)
                (cb* true))
              (cb* false)))))))

  (handle-local-updates-only [this local-cmds paths cb]
    (swap! *local-state #(reduce eval-cmd % local-cmds))
    (notify-subs this paths false)
    (when cb
      (cb true)))

  (<handle-sys-and-local-updates [this sys-cmds local-cmds paths cb]
    (au/go
      (let [ret (au/<? (<update-sys-state this sys-cmds))
            cb* (or cb (constantly nil))]
        (if-not ret
          (cb* false)
          (let [{:keys [prev-db-id cur-db-id updated-paths]} ret
                local-db-id @*cur-db-id
                notify-all? (not= prev-db-id local-db-id)]
            (if (or (nil? local-db-id)
                    (bc/earlier? local-db-id cur-db-id))
              (let [paths* (set/union (set paths) (set updated-paths))]
                (reset! *cur-db-id cur-db-id)
                (swap! *local-state #(reduce eval-cmd % local-cmds))
                (notify-subs this paths* notify-all?)
                (cb* true))
              (cb* false)))))))

  (start-update-loop [this]
    (ca/go-loop []
      (try
        (au/<? (<wait-for-conn-init this))
        (let [{:keys [cb] :as update} (au/<? update-ch)]
          (try
            (let [{:keys [sys-cmds local-cmds paths]} update]
              (case [(boolean (seq sys-cmds)) (boolean (seq local-cmds))]
                [false true]
                (handle-local-updates-only this local-cmds paths cb)

                [true false]
                (au/<? (<handle-sys-updates-only this sys-cmds paths cb))

                [true true]
                (au/<? (<handle-sys-and-local-updates
                        this sys-cmds local-cmds paths cb))

                [false false] ;; No cmds
                (when cb
                  (cb true))))
            (catch #?(:cljs js/Error :clj Throwable) e
              (if cb
                (cb e)
                (throw e)))))
        (catch #?(:cljs js/Error :clj Throwable) e
          (log-error (str "Exception in update loop: "
                          (u/ex-msg-and-stacktrace e)))))
      (recur)))

  (update-state! [this update-cmds cb]
    (when-not (sequential? update-cmds)
      (throw (ex-info "The update-cmds parameter must be a sequence."
                      (u/sym-map update-cmds))))
    (let [update-info (make-update-info update-cmds)]
      (ca/put! update-ch (assoc update-info :cb cb)))
    nil)

  (<update-sys-state [this sys-cmds]
    (let [cmds (mapv (fn [cmd]
                       (update cmd :arg #(u/edn->value-rec sys-state-schema
                                                           (:path cmd) %)))
                     sys-cmds)]
      (cc/<send-msg capsule-client :update-state {:update-cmds cmds}
                    update-state-timeout-ms)))

  (<get-in-sys-state [this db-id path]
    (au/go
      (or (sr/get state-cache [db-id path])
          (let [arg (u/sym-map db-id path)
                ret (au/<? (cc/<send-msg capsule-client :get-state arg
                                         get-state-timeout-ms))
                v (if (:vivo/is-forbidden ret)
                    :vivo/is-forbidden
                    (u/value-rec->edn (:v ret)))]
            (sr/put state-cache [db-id path] v)
            v))))

  (<handle-sys-state-changed [this arg metadata]
    ;; TODO: Combine this with handle-sys-updates-only
    (ca/go
      (try
        (let [{:keys [prev-db-id cur-db-id updated-paths]} arg
              local-db-id @*cur-db-id
              notify-all? (not= prev-db-id local-db-id)]
          (when (or (nil? local-db-id)
                    (bc/earlier? local-db-id cur-db-id))
            (reset! *cur-db-id cur-db-id)
            (notify-subs this updated-paths notify-all?)))
        (catch #?(:cljs js/Error :clj Throwable) e
          (log-error (str "Exception in <handle-sys-state-changed: "
                          (u/ex-msg-and-stacktrace e))))))))

(defn <init-conn
  [client sys-state-source log-error log-info *cur-db-id *conn-initialized?]
  (ca/go
    (try
      ;; TODO: Handle token-based login here
      (let [source* (or sys-state-source
                        {:temp-branch/db-id nil})
            db-id (au/<? (cc/<send-msg client :set-state-source source*))]
        (reset! *cur-db-id db-id)
        (reset! *conn-initialized? true)
        (log-info "State manager connection initialized."))
      (catch #?(:cljs js/Error :clj Throwable) e
        (log-error (str "Error initializing state manager client:\n"
                        (u/ex-msg-and-stacktrace e)))))))

(defn <on-connect
  [sys-state-source log-error log-info *cur-db-id *conn-initialized?
   capsule-client]
  (ca/go
    (try
      (au/<? (<init-conn capsule-client sys-state-source log-error log-info
                         *cur-db-id *conn-initialized?))
      (catch #?(:clj Exception :cljs js/Error) e
        (log-error (str "Error in <on-connect: "
                        (u/ex-msg-and-stacktrace e)))))))

(defn on-disconnect [*conn-initialized? capsule-client]
  (reset! *conn-initialized? false))

(defn check-sys-state-source [source]
  (when source
    (when-not (map? source)
      (throw (ex-info (str "sys-state-source must be a map. Got `"
                           source "`.")
                      {:sys-state-source source})))
    ;;  TODO check that only valid keys are present
    ))

(defn make-capsule-client
  [get-server-url sys-state-schema sys-state-source log-error log-info
   *cur-db-id *conn-initialized?]
  (when-not sys-state-schema
    (throw (ex-info (str "Missing `:sys-state-schema` option in state-manager "
                         "constructor.")
                    {})))
  (let [protocol (u/make-sm-server-protocol sys-state-schema)
        get-credentials (constantly {:subject-id "state-manager"
                                     :subject-secret ""})
        opts {:on-connect (partial <on-connect sys-state-source log-error
                                   log-info *cur-db-id *conn-initialized?)
              :on-disconnect (partial on-disconnect *conn-initialized?)}]
    (cc/client get-server-url get-credentials
               protocol :state-manager opts)))

(defn state-manager [opts]
  (let [{:keys [get-server-url
                initial-local-state
                log-error
                log-info
                state-cache-size
                sys-state-source
                sys-state-schema]} (merge default-sm-opts opts)
        *local-state (atom initial-local-state)
        *sub-id->sub (atom {})
        *cur-db-id (atom nil)
        *last-sub-id (atom 0)
        *conn-initialized? (atom (not get-server-url))
        *stopped? (atom false)
        state-cache (sr/stockroom state-cache-size)
        sm->op-cache (sr/stockroom 500)
        update-ch (ca/chan 50)
        capsule-client (when get-server-url
                         (check-sys-state-source sys-state-source)
                         (make-capsule-client
                          get-server-url sys-state-schema sys-state-source
                          log-error log-info *cur-db-id
                          *conn-initialized?))
        sm (->StateManager capsule-client sys-state-schema sys-state-source
                           log-info log-error state-cache sm->op-cache
                           update-ch *local-state *sub-id->sub *cur-db-id
                           *last-sub-id *conn-initialized? *stopped?)]
    (start-update-loop sm)
    (when get-server-url
      (cc/set-handler capsule-client :sys-state-changed
                      (partial <handle-sys-state-changed sm)))
    sm))
