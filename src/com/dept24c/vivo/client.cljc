(ns com.dept24c.vivo.client
  (:require
   [clojure.core.async :as ca]
   [clojure.set :as set]
   [clojure.string :as str]
   [com.dept24c.vivo.bristlecone.block-ids :as block-ids]
   [com.dept24c.vivo.commands :as commands]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.capsule.client :as cc]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.stockroom :as sr]
   [weavejester.dependency :as dep]))

(def default-vc-opts
  {:log-error println
   :log-info println
   :state-cache-num-keys 100})

(def get-state-timeout-ms 30000)
(def login-token-local-storage-key "login-token")
(def update-state-timeout-ms 30000)

(defn get-login-token []
  #?(:cljs
     (when (u/browser?)
       (.getItem (.-localStorage js/window) login-token-local-storage-key))))

(defn set-login-token [token]
  #?(:cljs
     (when (u/browser?)
       (.setItem (.-localStorage js/window)
                 login-token-local-storage-key token))))

(defn delete-login-token []
  #?(:cljs
     (when (u/browser?)
       (.removeItem (.-localStorage js/window) login-token-local-storage-key))))

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

(defn throw-missing-path-key
  ([k path sub-map]
   (throw-missing-path-key k path sub-map nil))
  ([k path sub-map subscriber-name]
   (throw (ex-info (str "Could not find a value for key `" k "` in path `"
                        path "` of " (if subscriber-name
                                       (str "component `" subscriber-name)
                                       (str "sub-map `" sub-map))
                        "`.")
                   (u/sym-map k path sub-map subscriber-name)))))

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

(defn check-path [path sub-syms sub-map]
  (if (= :vivo/subject-id path)
    path
    (reduce (fn [acc k]
              (when (and (symbol? k) (not (sub-syms k)))
                (throw (ex-info
                        (str "Path symbol `" k "` in path `" path "` is not "
                             "defined as a key in the subscription map.")
                        (u/sym-map path k sub-map))))
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
                              (u/sym-map sym sub-map))))
                    (if (= :vivo/subject-id path)
                      (update acc :sym->path assoc sym path)
                      (let [[head & tail] (check-path path sub-syms sub-map)
                            deps (filter symbol? path)]
                        (when-not (#{:local :conn :sys} head)
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
                            (let [path (sym->path sym)]
                              (conj acc [sym path])))
                          []
                          (concat (seq no-dep-syms)
                                  ordered-dep-syms))]
        (sr/put! sub-map->op-cache sub-map pairs)
        pairs)))

(defn get-sub-id [*last-sub-id]
  (swap! *last-sub-id (fn [sub-id]
                        (let [new-sub-id (inc sub-id)]
                          (if (> new-sub-id 1e9)
                            0
                            new-sub-id)))))

(defn update-sub?* [updated-paths sub-path]
  (reduce (fn [acc updated-path]
            (cond
              (= :vivo/subject-id sub-path)
              (if (= :vivo/subject-id updated-path)
                (reduced true)
                false)

              (= :vivo/subject-id updated-path)
              (if (= :vivo/subject-id sub-path)
                (reduced true)
                false)

              (some number? updated-path) ;; Numeric paths are complex...
              (reduced true)

              :else
              (let [[relationship _] (u/relationship-info
                                      (or updated-path [])
                                      (or sub-path []))]
                (if (= :sibling relationship)
                  false
                  (reduced true)))))
          false updated-paths))

(defn update-sub? [updated-paths sub-paths]
  (reduce (fn [acc sub-path]
            (if (or (and (sequential? sub-path) ;; Not :vivo/subject-id
                         (some number? sub-path)) ;; Numeric paths are complex..
                    (update-sub?* updated-paths sub-path))
              (reduced true)
              false))
          false sub-paths))

(defn resolve-path [acc ordered-pairs subscriber-name path]
  (when (sequential? path)
    (mapv (fn [k]
            (if (symbol? k)
              (or (get-in acc [:state k])
                  (throw-missing-path-key k path ordered-pairs subscriber-name))
              k))
          path)))

(defrecord VivoClient [capsule-client sys-state-schema sys-state-source
                       log-info log-error state-cache sub-map->op-cache
                       path->schema-cache update-ch subject-id-ch
                       *local-state *sub-id->sub *cur-db-id *last-sub-id
                       *conn-initialized? *stopped? *ssr-info *fp->schema
                       *subject-id]
  u/ISchemaStore
  (<fp->schema [this fp]
    (when-not (int? fp)
      (throw (ex-info (str "Given `fp` arg is not a `long`. Got `"
                           fp "`.")
                      {:given-fp fp})))
    (au/go
      (or (@*fp->schema fp)
          (let [pcf (au/<? (cc/<send-msg capsule-client :get-schema-pcf fp))
                schema (l/json->schema pcf)]
            (swap! *fp->schema assoc fp schema)
            schema))))

  (<schema->fp [this schema]
    (when-not (l/schema? schema)
      (throw (ex-info (str "Schema arg must be a Lancaster schema. Got `"
                           schema "`.")
                      {:given-schema schema})))
    (au/go
      (let [fp (l/fingerprint64 schema)]
        (when-not (@*fp->schema fp)
          (swap! *fp->schema assoc fp schema)
          (au/<? (cc/<send-msg capsule-client :store-schema-pcf
                               (l/pcf schema))))
        fp)))

  u/IVivoClient
  (<deserialize-value [this path ret]
    (au/go
      (when ret
        (let [value-sch (or (sr/get path->schema-cache path)
                            (let [sch (l/schema-at-path sys-state-schema path)]
                              (sr/put! path->schema-cache path sch)
                              sch))
              writer-sch (au/<? (u/<fp->schema this (:fp ret)))]
          (l/deserialize value-sch writer-sch (:bytes ret))))))

  (set-subject-id [this subject-id]
    (reset! *subject-id subject-id)
    (u/notify-subs this [:vivo/subject-id] false)
    nil)

  (log-in! [this identifier secret cb]
    (ca/go
      (try
        (sr/flush! state-cache)
        (let [arg (u/sym-map identifier secret)
              {:keys [subject-id token]} (au/<? (cc/<send-msg capsule-client
                                                              :log-in arg))
              ret (if-not subject-id
                    (do
                      (delete-login-token)
                      (log-info "Login failed.")
                      false)
                    (do
                      (set-login-token token)
                      (u/set-subject-id this subject-id)
                      (log-info "Login succeeded.")
                      true))]
          (when cb
            (cb ret)))
        (catch #?(:cljs js/Error :clj Throwable) e
          (log-error (str "Exception in log-in!" (u/ex-msg-and-stacktrace e)))
          (when cb
            (cb e))))))

  (log-out! [this]
    (ca/go
      (try
        (delete-login-token)
        (u/set-subject-id this nil)
        (sr/flush! state-cache)
        (let [ret (au/<? (cc/<send-msg capsule-client :log-out nil))]
          (log-info (str "Logout " (if ret "succeeded." "failed."))))
        (catch #?(:cljs js/Error :clj Throwable) e
          (log-error (str "Exception in log-out!"
                          (u/ex-msg-and-stacktrace e)))))))

  (shutdown! [this]
    (reset! *stopped? true)
    (cc/shutdown capsule-client)
    (log-info "Vivo client stopped."))

  (<make-state-info
    [this sub-map-or-ordered-pairs subscriber-name]
    (u/<make-state-info this sub-map-or-ordered-pairs subscriber-name
                        @*local-state @*cur-db-id))

  (<make-state-info
    [this sub-map-or-ordered-pairs subscriber-name local-state db-id]
    (au/go
      ;; TODO: Optimize by doing <get-in-sys-state calls in parallel
      ;;       where possible (non-dependent)
      (let [init {:state {}
                  :paths []}]
        (if-not (seq sub-map-or-ordered-pairs)
          init
          (let [ordered-pairs (if (map? sub-map-or-ordered-pairs)
                                (sub-map->ordered-pairs
                                 sub-map->op-cache sub-map-or-ordered-pairs)
                                sub-map-or-ordered-pairs)]
            ;; Use loop instead of reduce here to stay within the go block
            (loop [acc init
                   i 0]
              (let [[sym path] (nth ordered-pairs i)
                    resolved-path (resolve-path acc ordered-pairs subscriber-name
                                                path)
                    [path-head & path-tail] resolved-path
                    v (cond
                        (= :vivo/subject-id path)
                        @*subject-id

                        (= :local path-head)
                        (-> (commands/get-in-state local-state path-tail)
                            (:val))

                        (= :sys path-head)
                        (au/<? (u/<get-in-sys-state this db-id path-tail)))
                    new-acc (-> acc
                                (assoc-in [:state sym] v)
                                (update :paths conj (or resolved-path
                                                        path)))]
                (if (= (dec (count ordered-pairs)) i)
                  new-acc
                  (recur new-acc (inc i))))))))))

  (get-local-state [this sub-map subscriber-name]
    (let [ordered-pairs (sub-map->ordered-pairs sub-map->op-cache sub-map)
          local-state @*local-state]
      (reduce (fn [acc [sym path]]
                (if (= :vivo/subject-id path)
                  (assoc acc sym @*subject-id)
                  (let [resolved-path (mapv (fn [k]
                                              (if (symbol? k)
                                                (or (acc k)
                                                    (throw-missing-path-key
                                                     k path sub-map
                                                     subscriber-name))
                                                k))
                                            path)
                        v (->> (rest resolved-path)
                               (commands/get-in-state local-state)
                               (:val))]
                    (assoc acc sym v))))
              {} ordered-pairs)))

  (subscribe! [this sub-map cur-state update-fn* subscriber-name]
    (let [sub-id (get-sub-id *last-sub-id)
          ordered-pairs (sub-map->ordered-pairs sub-map->op-cache sub-map)
          <make-si (partial u/<make-state-info this ordered-pairs
                            subscriber-name)]
      (u/check-sub-map sub-id "subscriber" sub-map)
      (ca/go
        (try
          (when (au/<? (u/<wait-for-conn-init this))
            (let [{:keys [paths state]} (au/<? (<make-si))
                  update-fn (fn [local-state db-id]
                              (ca/go
                                (try
                                  (let [si (au/<? (<make-si local-state db-id))
                                        {uf-state :state} si]
                                    (when (and (not= cur-state uf-state)
                                               (@*sub-id->sub sub-id))
                                      (update-fn* uf-state)))
                                  (catch #?(:cljs js/Error :clj Throwable) e
                                    (log-error
                                     (str "Exception calling update-fn:\n"
                                          (u/ex-msg-and-stacktrace e)))))))
                  sub (u/sym-map paths update-fn)]
              (swap! *sub-id->sub assoc sub-id sub)
              (when-not (= cur-state state)
                (update-fn* state))))
          (catch #?(:cljs js/Error :clj Exception) e
            (log-error (str "Error in subscribe!\n"
                            (u/ex-msg-and-stacktrace e))))))
      sub-id))

  (unsubscribe! [this sub-id]
    (swap! *sub-id->sub
           (fn [m]
             (dissoc m sub-id)))
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
      (let [ret (au/<? (u/<update-sys-state this sys-cmds))
            cb* (or cb (constantly nil))]
        (if-not ret
          (cb* false)
          (let [{:keys [prev-db-id cur-db-id updated-paths]} ret
                local-db-id @*cur-db-id
                notify-all? (not= prev-db-id local-db-id)]
            (if (or (nil? local-db-id)
                    (block-ids/earlier? local-db-id cur-db-id))
              (do
                (reset! *cur-db-id cur-db-id)
                (u/notify-subs this updated-paths notify-all?)
                (cb* true))
              (cb* false)))))))

  (handle-local-updates-only [this local-cmds paths cb]
    (swap! *local-state #(reduce commands/eval-cmd % local-cmds))
    (u/notify-subs this paths false)
    (when cb
      (cb true)))

  (<handle-sys-and-local-updates [this sys-cmds local-cmds paths cb]
    (au/go
      (let [ret (au/<? (u/<update-sys-state this sys-cmds))
            cb* (or cb (constantly nil))]
        (if-not ret
          (cb* false)
          (let [{:keys [prev-db-id cur-db-id updated-paths]} ret
                local-db-id @*cur-db-id
                notify-all? (not= prev-db-id local-db-id)]
            (if (or (nil? local-db-id)
                    (block-ids/earlier? local-db-id cur-db-id))
              (let [paths* (set/union (set paths) (set updated-paths))]
                (reset! *cur-db-id cur-db-id)
                (swap! *local-state #(reduce commands/eval-cmd % local-cmds))
                (u/notify-subs this paths* notify-all?)
                (cb* true))
              (cb* false)))))))

  (start-update-loop [this]
    (ca/go-loop []
      (try
        (when (au/<? (u/<wait-for-conn-init this))
          (let [[update ch] (ca/alts! [update-ch subject-id-ch])]
            (if (= subject-id-ch ch)
              (u/set-subject-id this update)
              (try
                (let [{:keys [sys-cmds local-cmds paths cb]} update]
                  (case [(boolean (seq sys-cmds)) (boolean (seq local-cmds))]
                    [false true]
                    (u/handle-local-updates-only this local-cmds paths cb)

                    [true false]
                    (au/<? (u/<handle-sys-updates-only this sys-cmds paths cb))

                    [true true]
                    (au/<? (u/<handle-sys-and-local-updates
                            this sys-cmds local-cmds paths cb))

                    [false false] ;; No cmds
                    (when cb
                      (cb true))))
                (catch #?(:cljs js/Error :clj Throwable) e
                  (if-let [cb (:cb update)]
                    (cb e)
                    (throw e)))))))
        (catch #?(:cljs js/Error :clj Throwable) e
          (log-error (str "Exception in update loop: "
                          (u/ex-msg-and-stacktrace e)))))
      (when (au/<? (u/<wait-for-conn-init this)) ;; If stopped, exit loop
        (recur))))

  (update-state! [this update-cmds cb]
    (try
      (when-not (sequential? update-cmds)
        (throw (ex-info "The update-cmds parameter must be a sequence."
                        (u/sym-map update-cmds))))
      (let [update-info (make-update-info update-cmds)]
        (ca/put! update-ch (assoc update-info :cb cb)))
      (catch #?(:clj Exception :cljs js/Error) e
        (if cb
          (cb e)
          (throw e))))
    nil)

  (<update-sys-state [this sys-cmds]
    (cc/<send-msg capsule-client :update-state
                  (map (partial u/update-cmd->serializable-update-cmd this)
                       sys-cmds)
                  update-state-timeout-ms))

  (update-cmd->serializable-update-cmd [this cmd]
    (if-not (:arg cmd)
      cmd
      (let [{:keys [arg path]} cmd
            arg-sch (or (sr/get path->schema-cache path)
                        (let [sch (l/schema-at-path sys-state-schema path)]
                          (sr/put! path->schema-cache path sch)
                          sch))
            fp (l/fingerprint64 arg-sch)
            bytes (l/serialize arg-sch (:arg cmd))
            scmd (assoc cmd :arg (u/sym-map fp bytes))]
        (swap! *fp->schema assoc fp arg-sch)
        scmd)))

  (<get-in-sys-state [this db-id path]
    (au/go
      (or (sr/get state-cache [db-id path])
          (let [arg (u/sym-map db-id path)
                ret (au/<? (cc/<send-msg capsule-client :get-state arg
                                         get-state-timeout-ms))
                v (if (= :vivo/unauthorized ret)
                    :vivo/unauthorized
                    (when ret
                      (au/<? (u/<deserialize-value this path ret))))]
            (sr/put! state-cache [db-id path] v)
            v))))

  (<handle-sys-state-changed [this arg metadata]
    ;; TODO: Combine this with handle-sys-updates-only
    (ca/go
      (try
        (let [{:keys [prev-db-id cur-db-id updated-paths]} arg
              local-db-id @*cur-db-id
              notify-all? (not= prev-db-id local-db-id)]
          (when (or (nil? local-db-id)
                    (block-ids/earlier? local-db-id cur-db-id))
            (reset! *cur-db-id cur-db-id)
            (u/notify-subs this updated-paths notify-all?)))
        (catch #?(:cljs js/Error :clj Throwable) e
          (log-error (str "Exception in <handle-sys-state-changed: "
                          (u/ex-msg-and-stacktrace e)))))))

  (<add-subject! [this identifier secret]
    (u/<add-subject! this identifier secret) nil)

  (<add-subject! [this identifier secret subject-id]
    (cc/<send-msg capsule-client :add-subject
                  (u/sym-map identifier secret subject-id))))

(defn <log-in-w-token [capsule-client subject-id-ch log-info token]
  (au/go
    (if-let [subject-id (au/<? (cc/<send-msg capsule-client
                                             :log-in-w-token token))]
      (do
        (ca/put! subject-id-ch subject-id)
        (log-info "Token-based login succeeded."))
      (do
        (delete-login-token)
        (log-info "Token-based login failed")))))

(defn <init-conn
  [capsule-client sys-state-source log-error log-info *cur-db-id
   *conn-initialized? subject-id-ch]
  (ca/go
    (try
      (let [db-id (au/<? (cc/<send-msg capsule-client
                                       :set-state-source sys-state-source))]
        (when-let [token (get-login-token)]
          (au/<? (<log-in-w-token capsule-client subject-id-ch log-info token)))
        (reset! *cur-db-id db-id)
        (reset! *conn-initialized? true)
        (log-info "Vivo client connection initialized."))
      (catch #?(:cljs js/Error :clj Throwable) e
        (log-error (str "Error initializing vivo client:\n"
                        (u/ex-msg-and-stacktrace e)))))))

(defn <on-connect
  [sys-state-source log-error log-info *cur-db-id *conn-initialized?
   subject-id-ch capsule-client]
  (ca/go
    (try
      (au/<? (<init-conn capsule-client sys-state-source log-error log-info
                         *cur-db-id *conn-initialized? subject-id-ch))
      (catch #?(:clj Exception :cljs js/Error) e
        (log-error (str "Error in <on-connect: "
                        (u/ex-msg-and-stacktrace e)))))))

(defn on-disconnect [*conn-initialized? capsule-client]
  (reset! *conn-initialized? false))

(defn check-sys-state-source [sys-state-source]
  ;; sys-state-source must be either:
  ;; - {:branch/name <branch-name>}
  ;; - {:temp-branch/db-id <db-id> or nil}
  (when-not (map? sys-state-source)
    (throw (ex-info (str "sys-state-source must be a map. Got `"
                         sys-state-source "`.")
                    (u/sym-map sys-state-source))))
  (if-let [branch-name (:branch/name sys-state-source)]
    (when-not (string? branch-name)
      (throw (ex-info (str "Bad :branch/name value in :sys-state-source. "
                           "Expected a string, got `" branch-name "`.")
                      (u/sym-map sys-state-source branch-name))))
    (if (contains? sys-state-source :temp-branch/db-id)
      (let [db-id (:temp-branch/db-id sys-state-source)]
        (when-not (or (nil? db-id)
                      (string? db-id))
          (throw (ex-info
                  (str "Bad :temp-branch/db-id value in :sys-state-source. "
                       "Expected a string or nil, got `" db-id "`.")
                  (u/sym-map sys-state-source db-id)))))
      (throw (ex-info
              (str ":sys-state-source must contain either a :branch/name key "
                   "or a :temp-branch/db-id key. Got `" sys-state-source "`.")
              (u/sym-map sys-state-source))))))

(defn make-capsule-client
  [get-server-url sys-state-schema sys-state-source log-error log-info
   *cur-db-id *conn-initialized? subject-id-ch]
  (when-not sys-state-schema
    (throw (ex-info (str "Missing `:sys-state-schema` option in vivo-client "
                         "constructor.")
                    {})))
  (let [get-credentials (constantly {:subject-id "vivo-client"
                                     :subject-secret ""})
        opts {:on-connect (partial <on-connect sys-state-source log-error
                                   log-info *cur-db-id *conn-initialized?
                                   subject-id-ch)
              :on-disconnect (partial on-disconnect *conn-initialized?)}]
    (cc/client get-server-url get-credentials
               u/client-server-protocol :client opts)))

(defn vivo-client [opts]
  (let [{:keys [get-server-url
                initial-local-state
                log-error
                log-info
                state-cache-num-keys
                sys-state-source
                sys-state-schema]} (merge default-vc-opts opts)
        *local-state (atom initial-local-state)
        *sub-id->sub (atom {})
        *cur-db-id (atom nil)
        *last-sub-id (atom 0)
        *conn-initialized? (atom (not get-server-url))
        *stopped? (atom false)
        *ssr-info (atom nil)
        *fp->schema (atom {})
        *subject-id (atom nil)
        path->schema-cache (sr/stockroom 100)
        state-cache (sr/stockroom state-cache-num-keys)
        sub-map->op-cache (sr/stockroom 500)
        update-ch (ca/chan 50)
        subject-id-ch (ca/chan 10)
        capsule-client (when get-server-url
                         (check-sys-state-source sys-state-source)
                         (make-capsule-client
                          get-server-url sys-state-schema sys-state-source
                          log-error log-info *cur-db-id *conn-initialized?
                          subject-id-ch))
        vc (->VivoClient capsule-client sys-state-schema sys-state-source
                         log-info log-error state-cache sub-map->op-cache
                         path->schema-cache update-ch subject-id-ch
                         *local-state *sub-id->sub *cur-db-id *last-sub-id
                         *conn-initialized? *stopped? *ssr-info *fp->schema
                         *subject-id)]
    (u/start-update-loop vc)
    (when get-server-url
      (cc/set-handler capsule-client :sys-state-changed
                      (partial u/<handle-sys-state-changed vc))
      (cc/set-handler capsule-client :get-schema-pcf
                      (fn [fp metadata]
                        (if-let [schema (@*fp->schema fp)]
                          (l/pcf schema)
                          (do
                            (log-error
                             (str "Could not find PCF for fingerprint `"
                                  fp "`."))
                            nil)))))
    vc))
