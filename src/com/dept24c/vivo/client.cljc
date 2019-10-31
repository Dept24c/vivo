(ns com.dept24c.vivo.client
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [com.dept24c.vivo.bristlecone.block-ids :as block-ids]
   [com.dept24c.vivo.commands :as commands]
   [com.dept24c.vivo.react :as react]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.capsule.client :as cc]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.stockroom :as sr]))

(def default-vc-opts
  {:log-error println
   :log-info println
   :state-cache-num-keys 500
   :sys-state-cache-num-keys 100})

(def get-state-timeout-ms 30000)
(def initial-ssr-info {:resolved {} :needed #{}})
(def login-token-local-storage-key "login-token")
(def max-commit-attempts 100)
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

(defn make-update-info [update-cmds]
  (reduce
   (fn [acc cmd]
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
               (u/throw-bad-path-root path))]
       (update acc k conj (assoc cmd :path path))))
   {:local-cmds []
    :sys-cmds []}
   update-cmds))

(defn get-sub-id [*last-sub-id]
  (str (swap! *last-sub-id (fn [sub-id]
                             (let [new-sub-id (inc sub-id)]
                               (if (> new-sub-id 1e9)
                                 0
                                 new-sub-id))))))

(defn get-in-state [state path prefix]
  (if-not (some sequential? path)
    (:val (commands/get-in-state state path prefix))
    (when-not (u/empty-sequence-in-path? path)
      (map #(get-in-state state % prefix) (u/expand-path path)))))

(defn eval-cmds [initial-state cmds prefix]
  (reduce (fn [{:keys [state] :as acc} cmd]
            (let [ret (commands/eval-cmd state cmd prefix)]
              (-> acc
                  (assoc :state (:state ret))
                  (update :update-infos conj (:update-info ret)))))
          {:state initial-state
           :update-infos []}
          cmds))

(defn xf-res-map [res-map]
  (reduce-kv (fn [acc k v]
               (assoc acc k (when-not (fn? v)
                              v)))
             {} res-map))

(defrecord VivoClient [capsule-client sys-state-schema sys-state-source
                       log-info log-error rpc-name-kw->info state-cache
                       sys-state-cache sub-map->op-cache
                       path->schema-cache updates-ch updates-pub
                       set-subject-id! *local-state *cur-db-id
                       *conn-initialized? *stopped? *ssr-info
                       *fp->schema *subject-id]
  u/ISchemaStore
  (<fp->schema [this fp]
    (when-not (int? fp)
      (throw (ex-info (str "Given `fp` arg is not a `long`. Got `"
                           fp "`.")
                      {:given-fp fp})))
    (au/go
      (or (@*fp->schema fp)
          (let [pcf (au/<? (cc/<send-msg capsule-client
                                         :get-schema-pcf fp))
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
  (ssr? [this]
    (boolean @*ssr-info))

  (ssr-get-state! [this sub-map resolution-map]
    (or (get (:resolved @*ssr-info) [sub-map (xf-res-map resolution-map)])
        (do
          (swap! *ssr-info update :needed conj [sub-map resolution-map])
          nil)))

  (<ssr [this component-fn component-name static-markup?]
    #?(:cljs
       (au/go
         (when-not (ifn? component-fn)
           (throw (ex-info (str "component-fn must be a function. Got: `"
                                (or component-fn "nil") "`.")
                           (u/sym-map component-fn))))
         (when-not (compare-and-set! *ssr-info nil initial-ssr-info)
           (throw
            (ex-info (str "Another SSR is in progress. Try again...") {})))
         (try
           (loop []
             (let [el (component-fn this)
                   _ (when-not (react/valid-element? el)

                       (throw (ex-info
                               (str "component-fn must return a valid React "
                                    "element. Returned: `" (or el "nil") "`.")
                               {:returned el})))
                   s (if static-markup?
                       (react/render-to-static-markup el)
                       (react/render-to-string el))
                   {:keys [needed]} @*ssr-info]
               (if-not (seq needed)
                 s
                 (do
                   (doseq [[sub-map resolution-map] needed]
                     (let [{:keys [state]} (au/<? (u/<make-state-info
                                                   this sub-map
                                                   component-name
                                                   resolution-map))]
                       (swap! *ssr-info update
                              :resolved assoc
                              [sub-map (xf-res-map resolution-map)] state)))
                   (swap! *ssr-info assoc :needed #{})
                   (recur)))))
           (finally
             (reset! *ssr-info nil))))))

  (get-cached-state [this sub-map resolution-map]
    (sr/get state-cache [sub-map (xf-res-map resolution-map)]))

  (get-local-state [this sub-map resolution-map component-name]
    (let [ordered-pairs (u/sub-map->ordered-pairs sub-map->op-cache sub-map)
          local-state @*local-state]
      (-> (reduce
           (fn [acc [sym path]]
             (let [v (cond
                       (= :vivo/subject-id path)
                       @*subject-id

                       (= :local (first path))
                       (let [resolved-path (mapv
                                            (fn [k]
                                              (if-not (symbol? k)
                                                k
                                                (acc k)))
                                            path)]
                         (get-in-state local-state resolved-path :local)))]
               (assoc acc sym v)))
           resolution-map ordered-pairs)
          (select-keys (keys sub-map)))))

  (<deserialize-value [this path ret]
    (au/go
      (when ret
        (let [schema-path (rest path) ; Ignore :sys
              value-sch (u/path->schema path->schema-cache sys-state-schema
                                        schema-path)
              writer-sch (au/<? (u/<fp->schema this (:fp ret)))]
          (l/deserialize value-sch writer-sch (:bytes ret))))))

  (log-in! [this identifier secret cb]
    (when-not capsule-client
      (throw
       (ex-info (str "Can't log in because the :get-server-url "
                     "option was not provided when the vivo-client was "
                     "created.") {})))
    (ca/go
      (try
        (sr/flush! state-cache)
        (sr/flush! sys-state-cache)
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
                      (set-subject-id! subject-id)
                      (log-info "Login succeeded.")
                      true))]
          (when cb
            (cb ret)))
        (catch #?(:cljs js/Error :clj Throwable) e
          (log-error (str "Exception in log-in!" (u/ex-msg-and-stacktrace e)))
          (when cb
            (cb e))))))

  (log-out! [this]
    (when-not capsule-client
      (throw
       (ex-info (str "Can't log in because the :get-server-url "
                     "option was not provided when the vivo-client was "
                     "created.") {})))
    (ca/go
      (try
        (delete-login-token)
        (set-subject-id! nil)
        (sr/flush! state-cache)
        (sr/flush! sys-state-cache)
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
    [this sub-map-or-ordered-pairs subscriber-name resolution-map]
    (u/<make-state-info this sub-map-or-ordered-pairs subscriber-name
                        resolution-map @*local-state @*cur-db-id))

  (<make-state-info
    [this sub-map-or-ordered-pairs subscriber-name resolution-map
     local-state db-id]
    (au/go
      ;; TODO: Optimize by doing <get-in-sys-state calls in parallel
      ;;       where possible (non-dependent)
      (let [init {:state resolution-map
                  :paths []}]
        (if-not (seq sub-map-or-ordered-pairs)
          init
          (let [ordered-pairs (if (map? sub-map-or-ordered-pairs)
                                (u/sub-map->ordered-pairs
                                 sub-map->op-cache sub-map-or-ordered-pairs)
                                sub-map-or-ordered-pairs)
                sub-keys (map first ordered-pairs)]
            ;; Use loop instead of reduce here to stay within the go block
            (loop [acc init
                   i 0]
              (let [[sym path] (nth ordered-pairs i)
                    resolved-path (u/resolve-symbols-in-path
                                   acc ordered-pairs subscriber-name path)
                    [path-head & path-tail] resolved-path
                    v (cond
                        (= :vivo/subject-id path)
                        @*subject-id

                        (some nil? resolved-path)
                        nil

                        (= :local path-head)
                        (get-in-state local-state resolved-path :local)


                        (= :sys path-head)
                        (when (and db-id (not @*stopped?))
                          (au/<? (u/<get-in-sys-state this db-id
                                                      resolved-path))))
                    path* (or resolved-path path)
                    paths (cond
                            (not (sequential? path*)) [path*]
                            (some sequential? path*) (u/expand-path path*)
                            :else [path*])
                    new-acc (-> acc
                                (assoc-in [:state sym] v)
                                (update :paths concat paths))]
                (if (= (dec (count ordered-pairs)) i)
                  (update new-acc :state select-keys sub-keys)
                  (recur new-acc (inc i))))))))))

  (subscribe!
    [this sub-map initial-state update-fn* subscriber-name resolution-map]
    (u/check-sub-map subscriber-name "subscriber" sub-map)
    (let [ordered-pairs (u/sub-map->ordered-pairs sub-map->op-cache sub-map)
          <make-si (partial u/<make-state-info this ordered-pairs
                            subscriber-name resolution-map)
          update-sub-ch (ca/chan 10)
          unsubscribe-ch (ca/promise-chan)
          unsubscribe! #(ca/put! unsubscribe-ch true)]
      (ca/sub updates-pub :all update-sub-ch)
      (ca/go
        (try
          (when (au/<? (u/<wait-for-conn-init this))
            (let [{cur-state :state
                   cur-paths :paths} (au/<? (<make-si))
                  *last-state (atom cur-state)]
              (sr/put! state-cache [sub-map (xf-res-map resolution-map)]
                       cur-state)
              (when (not= initial-state cur-state)
                (update-fn* cur-state))
              (loop [paths-to-watch cur-paths]
                (let [[v ch] (au/alts? [update-sub-ch unsubscribe-ch])]
                  (when-not @*stopped?
                    (if (= unsubscribe-ch ch)
                      (ca/unsub updates-pub :all update-sub-ch)
                      (let [{:keys [update-infos
                                    notify-all? db-id local-state]
                             :or {db-id @*cur-db-id
                                  local-state @*local-state}} v
                            new-paths (if-not (or notify-all?
                                                  (u/update-sub?
                                                   update-infos
                                                   paths-to-watch))
                                        paths-to-watch
                                        (let [si (au/<? (<make-si
                                                         local-state db-id))
                                              {:keys [state paths]} si]
                                          (when (not= @*last-state state)
                                            (reset! *last-state state)
                                            (update-fn* state))
                                          paths))]
                        (recur new-paths))))))))
          (catch #?(:cljs js/Error :clj Exception) e
            (log-error (str "Error in subscribe!\n"
                            (u/ex-msg-and-stacktrace e))))))
      unsubscribe!))

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

  (<handle-updates [this update-info cb*]
    (ca/go
      (when (au/<? (u/<wait-for-conn-init this))
        (let [{:keys [sys-cmds local-cmds]} update-info
              cb (or cb* (constantly nil))
              sys-ret (when (seq sys-cmds)
                        (when-let [ret (au/<? (u/<update-sys-state
                                               this sys-cmds))]
                          (if (= :vivo/unauthorized ret)
                            ret
                            (let [{:keys [new-db-id
                                          prev-db-id update-infos]} ret
                                  local-db-id @*cur-db-id
                                  notify-all? (not= prev-db-id local-db-id)]
                              (when (or (nil? local-db-id)
                                        (block-ids/earlier? local-db-id
                                                            new-db-id))
                                (reset! *cur-db-id new-db-id))
                              (u/sym-map notify-all? update-infos)))))]
          (if (and (seq sys-cmds)
                   (or (not sys-ret)
                       (= :vivo/unauthorized sys-ret)))
            (cb sys-ret) ;; Don't do local/sub updates if sys updates failed
            (loop [num-attempts 1]
              (let [cur-local-state @*local-state
                    local-ret (eval-cmds cur-local-state local-cmds :local)
                    local-state (:state local-ret)]
                (if (compare-and-set! *local-state cur-local-state
                                      local-state)
                  (let [update-infos (concat (:update-infos sys-ret)
                                             (:update-infos local-ret))
                        {:keys [notify-all?]} sys-ret]
                    (ca/put! updates-ch (u/sym-map update-infos notify-all?
                                                   local-state))
                    (cb true))
                  (if (< num-attempts max-commit-attempts)
                    (recur (inc num-attempts))
                    (cb (ex-info
                         (str "Failed to commit updates after "
                              num-attempts " attempts.")
                         (u/sym-map max-commit-attempts
                                    sys-cmds local-cmds))))))))))))

  (update-state! [this update-cmds cb]
    (when-not (sequential? update-cmds)
      (when cb
        (cb (ex-info "The update-cmds parameter must be a sequence."
                     (u/sym-map update-cmds)))))
    (let [update-info (make-update-info update-cmds)]
      (u/<handle-updates this update-info cb))
    nil)

  (<update-sys-state [this sys-cmds]
    (au/go
      (when-not capsule-client
        (throw
         (ex-info (str "Can't update :sys state because the :get-server-url "
                       "option was not provided when the vivo-client was "
                       "created.") {})))
      (let [ch (ca/merge
                (map-indexed
                 #(u/<update-cmd->serializable-update-cmd this %1 %2)
                 sys-cmds))
            ;; Use i->v map to preserve original command order
            sucs-map (au/<? (ca/reduce (fn [acc [i suc]]
                                         (assoc acc i suc))
                                       {} ch))
            sucs (map sucs-map (range (count sys-cmds)))]
        (au/<? (cc/<send-msg capsule-client :update-state
                             sucs update-state-timeout-ms)))))

  (<update-cmd->serializable-update-cmd [this i cmd]
    (au/go
      (if-not (:arg cmd)
        [i cmd]
        (let [{:keys [arg path]} cmd
              arg-sch (or (sr/get path->schema-cache path)
                          (let [schema-path (rest path) ; Ignore :sys
                                sch (l/schema-at-path sys-state-schema
                                                      schema-path)]
                            (sr/put! path->schema-cache path sch)
                            sch))
              fp (au/<? (u/<schema->fp this arg-sch))
              bytes (l/serialize arg-sch (:arg cmd))
              scmd (assoc cmd :arg (u/sym-map fp bytes))]
          (swap! *fp->schema assoc fp arg-sch)
          [i scmd]))))

  (<get-in-sys-state [this db-id path]
    (when-not capsule-client
      (throw
       (ex-info (str "Can't get :sys state because the :get-server-url "
                     "option was not provided when the vivo-client was "
                     "created.") {})))
    (au/go
      (or (sr/get sys-state-cache [db-id path])
          (when-not @*stopped?
            (let [arg (u/sym-map db-id path)
                  ret (au/<? (cc/<send-msg capsule-client :get-state arg
                                           get-state-timeout-ms))
                  v (if (= :vivo/unauthorized ret)
                      :vivo/unauthorized
                      (when ret
                        (au/<? (u/<deserialize-value this path ret))))]
              (sr/put! sys-state-cache [db-id path] v)
              v)))))

  (handle-sys-state-changed [this arg metadata]
    (try
      (let [{:keys [prev-db-id new-db-id update-infos]} arg
            local-db-id @*cur-db-id
            notify-all? (not= prev-db-id local-db-id)]
        (when (or (nil? local-db-id)
                  (block-ids/earlier? local-db-id new-db-id))
          (reset! *cur-db-id new-db-id)
          (ca/put! updates-ch (u/sym-map update-infos notify-all?))))
      (catch #?(:cljs js/Error :clj Throwable) e
        (log-error (str "Exception in <handle-sys-state-changed: "
                        (u/ex-msg-and-stacktrace e))))))

  (<add-subject! [this identifier secret]
    (u/<add-subject! this identifier secret) nil)

  (<add-subject! [this identifier secret subject-id]
    (cc/<send-msg capsule-client :add-subject
                  (u/sym-map identifier secret subject-id)))

  (<rpc [this rpc-name-kw arg timeout-ms]
    (au/go
      (when-not (keyword? rpc-name-kw)
        (throw (ex-info (str "rpc-name-kw must be a keyword. Got`" rpc-name-kw
                             "`.")
                        (u/sym-map rpc-name-kw arg))))
      (let [rpc-info (rpc-name-kw->info rpc-name-kw)
            _ (when-not rpc-info
                (throw (ex-info
                        (str "No RPC with name `" rpc-name-kw "` is registered."
                             " Either this is a typo or you need to add `"
                             rpc-name-kw "` to the :rpcs map when creating the "
                             "Vivo client.")
                        {:known-rpcs (keys rpc-name-kw->info)
                         :given-rpc rpc-name-kw})))
            {:keys [arg-schema ret-schema]} rpc-info
            arg {:rpc-name-kw-ns (namespace rpc-name-kw)
                 :rpc-name-kw-name (name rpc-name-kw)
                 :arg {:fp (au/<? (u/<schema->fp this arg-schema))
                       :bytes (l/serialize arg-schema arg)}}
            ret* (au/<? (cc/<send-msg capsule-client :rpc arg timeout-ms))]
        (cond
          (nil? ret*)
          nil

          (= :vivo/unauthorized ret*)
          (throw (ex-info
                  (str "RPC `" rpc-name-kw "` is unauthorized "
                       "for this user.")
                  {:rpc-name-kw rpc-name-kw
                   :subject-id @*subject-id}))

          :else
          (let [{:keys [fp bytes]} ret*
                w-schema (au/<? (u/<fp->schema this fp))]
            (l/deserialize ret-schema w-schema bytes)))))))

(defn <log-in-w-token [capsule-client set-subject-id! log-info token]
  (au/go
    (if-let [subject-id (au/<? (cc/<send-msg capsule-client
                                             :log-in-w-token token))]
      (do
        (set-subject-id! subject-id)
        (log-info "Token-based login succeeded."))
      (do
        (delete-login-token)
        (log-info "Token-based login failed")))))

(defn <init-conn
  [capsule-client sys-state-source log-error log-info *cur-db-id
   *conn-initialized? set-subject-id!]
  (ca/go
    (try
      (let [db-id (au/<? (cc/<send-msg capsule-client :set-state-source
                                       sys-state-source))]
        (when-let [token (get-login-token)]
          (au/<? (<log-in-w-token capsule-client set-subject-id!
                                  log-info token)))
        (reset! *cur-db-id db-id)
        (reset! *conn-initialized? true)
        (log-info "Vivo client connection initialized."))
      (catch #?(:cljs js/Error :clj Throwable) e
        (log-error (str "Error initializing vivo client:\n"
                        (u/ex-msg-and-stacktrace e)))))))

(defn <on-connect
  [sys-state-source log-error log-info *cur-db-id *conn-initialized?
   set-subject-id! capsule-client]
  (ca/go
    (try
      (au/<? (<init-conn capsule-client sys-state-source log-error log-info
                         *cur-db-id *conn-initialized? set-subject-id!))
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
   *cur-db-id *conn-initialized? set-subject-id!]
  (when-not sys-state-schema
    (throw (ex-info (str "Missing `:sys-state-schema` option in vivo-client "
                         "constructor.")
                    {})))
  (let [get-credentials (constantly {:subject-id "vivo-client"
                                     :subject-secret ""})
        opts {:on-connect (partial <on-connect sys-state-source log-error
                                   log-info *cur-db-id *conn-initialized?
                                   set-subject-id!)
              :on-disconnect (partial on-disconnect *conn-initialized?)}]
    (cc/client get-server-url get-credentials
               u/client-server-protocol :client opts)))



(defn vivo-client [opts]
  (let [{:keys [get-server-url
                initial-local-state
                log-error
                log-info
                rpc-name-kw->info
                state-cache-num-keys
                sys-state-cache-num-keys
                sys-state-source
                sys-state-schema]} (merge default-vc-opts opts)
        *local-state (atom initial-local-state)
        *cur-db-id (atom nil)
        *conn-initialized? (atom (not get-server-url))
        *stopped? (atom false)
        *ssr-info (atom nil)
        *fp->schema (atom {})
        *subject-id (atom nil)

        updates-ch (ca/chan (ca/sliding-buffer 100))
        updates-pub (ca/pub updates-ch (constantly :all))
        set-subject-id! (fn [subject-id]
                          (reset! *subject-id subject-id)
                          (ca/put! updates-ch
                                   {:update-infos [{:norm-path :vivo/subject-id
                                                    :op :set}]})
                          nil)
        path->schema-cache (sr/stockroom 100)
        state-cache (sr/stockroom state-cache-num-keys)
        sys-state-cache (sr/stockroom sys-state-cache-num-keys)
        sub-map->op-cache (sr/stockroom 500)
        _ (when rpc-name-kw->info
            (u/check-rpc-name-kw->info rpc-name-kw->info))
        capsule-client (when get-server-url
                         (check-sys-state-source sys-state-source)
                         (make-capsule-client
                          get-server-url sys-state-schema sys-state-source
                          log-error log-info *cur-db-id *conn-initialized?
                          set-subject-id!))
        vc (->VivoClient capsule-client sys-state-schema sys-state-source
                         log-info log-error rpc-name-kw->info state-cache
                         sys-state-cache sub-map->op-cache
                         path->schema-cache updates-ch updates-pub
                         set-subject-id! *local-state *cur-db-id
                         *conn-initialized? *stopped? *ssr-info
                         *fp->schema *subject-id)]
    (when get-server-url
      (cc/set-handler capsule-client :sys-state-changed
                      (partial u/handle-sys-state-changed vc))
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
