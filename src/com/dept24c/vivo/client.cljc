(ns com.dept24c.vivo.client
  (:require
   [clojure.core.async :as ca]
   [com.dept24c.vivo.bristlecone.block-ids :as block-ids]
   [com.dept24c.vivo.commands :as commands]
   [com.dept24c.vivo.react :as react]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.client :as cc]
   [deercreeklabs.capsule.logging :as log]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.stockroom :as sr]))

(def default-vc-opts
  {:log-error println
   :log-info println
   :sys-state-cache-num-keys 100})

(def get-state-timeout-ms 30000)
(def initial-ssr-info {:resolved {} :needed #{}})
(def max-commit-attempts 100)
(def update-state-timeout-ms 30000)

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

(defn count-at-path [state path prefix]
  (let [coll (:val (commands/get-in-state state path prefix))]
    (cond
      (or (map? coll) (sequential? coll))
      (count coll)

      (nil? coll)
      0

      :else
      (throw
       (ex-info
        (str "`:vivo/count` terminates path, but there is not a collection at "
             path ".")
        {:path path
         :value coll})))))

(defn do-concat [state path prefix]
  (let [seqs (:val (commands/get-in-state state path prefix))]
    (when (or (not (sequential? seqs))
              (not (sequential? (first seqs))))
      (throw
       (ex-info
        (str "`:vivo/concat` terminates path, but there "
             "is not a sequence of sequences at " path ".")
        {:path path
         :value seqs})))
    (apply concat seqs)))

(defn <ks-at-path [kw state path prefix full-path]
  (au/go
    (let [coll (:val (commands/get-in-state state path prefix))]
      (cond
        (map? coll)
        (keys coll)

        (sequential? coll)
        (range (count coll))

        (nil? coll)
        []

        :else
        (throw
         (ex-info
          (str "`" kw "` is in the path, but "
               "there is not a collection at " path ".")
          {:full-path full-path
           :missing-collection-path path
           :value coll}))))))

(defn <get-state-and-expanded-path [state path prefix]
  (au/go
    (let [last-path-k (last path)
          <ks-at-path* #(<ks-at-path :vivo/* state % prefix path)
          join? (u/has-join? path)
          term-kw? (u/terminal-kw? last-path-k)]
      (cond
        (u/empty-sequence-in-path? path)
        [nil [path]]

        (and (not term-kw?) (not join?))
        (let [{:keys [norm-path val]} (commands/get-in-state state path prefix)]
          [val [norm-path]])

        (and term-kw? (not join?))
        (let [path* (butlast path)
              val (case last-path-k
                    :vivo/keys (au/<? (<ks-at-path :vivo/keys state path* prefix
                                                   path))
                    :vivo/count (count-at-path state path* prefix)
                    :vivo/concat (do-concat state path* prefix))]
          [val [path]])

        (and (not term-kw?) join?)
        (let [xpath (au/<? (u/<expand-path <ks-at-path* path))
              num-results (count xpath)]
          ;; Use loop to stay in go block
          (loop [out []
                 i 0]
            (let [path* (nth xpath i)
                  ret (au/<? (<get-state-and-expanded-path
                              state path* prefix))
                  new-out (conj out (first ret))
                  new-i (inc i)]
              (if (not= num-results new-i)
                (recur new-out new-i)
                [new-out xpath]))))

        (and term-kw? join?)
        (let [xpath (au/<? (u/<expand-path <ks-at-path* (butlast path)))
              num-results (count xpath)
              results (loop [out [] ;; Use loop to stay in go block
                             i 0]
                        (let [path* (nth xpath i)
                              ret (au/<? (<get-state-and-expanded-path
                                          state path* prefix))
                              new-out (conj out (first ret))
                              new-i (inc i)]
                          (if (not= num-results new-i)
                            (recur new-out new-i)
                            new-out)))
              v (case last-path-k
                  :vivo/keys (range (count results))
                  :vivo/count (count results)
                  :vivo/concat (apply concat results))]
          [v xpath])))))

(defn eval-cmds [initial-state cmds prefix]
  (reduce (fn [{:keys [state] :as acc} cmd]
            (let [ret (commands/eval-cmd state cmd prefix)]
              (-> acc
                  (assoc :state (:state ret))
                  (update :update-infos conj (:update-info ret)))))
          {:state initial-state
           :update-infos []}
          cmds))

(defn atom? [x]
  #?(:clj (instance? clojure.lang.IAtom x)
     :cljs (satisfies? IAtom x)))

(defn js-object? [v]
  #?(:cljs (object? v)
     :clj (throw (ex-info "js-object? is not supported in clj." {}))))

(defn strip-non-vals [v]
  (cond
    (fn? v) nil
    (atom? v) nil
    (js-object? v) nil
    (map? v) (reduce-kv (fn [acc k v*]
                          (assoc acc k (strip-non-vals v*)))
                        {} v)
    (sequential? v) (reduce (fn [acc v*]
                              (conj acc (strip-non-vals v*)))
                            [] v)
    (set? v) (reduce (fn [acc v*]
                       (conj acc (strip-non-vals v*)))
                     #{} v)
    :else v))

(defn ssr-get-state!* [*ssr-info sub-map resolution-map]
  (let [stripped-rm (strip-non-vals resolution-map)
        v (get (:resolved @*ssr-info) [sub-map stripped-rm] :not-found)]
    (if (not= :not-found v)
      v
      (do
        (swap! *ssr-info update :needed conj [sub-map resolution-map])
        nil))))

(defn <subscription-loop
  [update-fn cur-paths update-sub-ch unsub-ch updates-pub <make-si *local-state
   *stopped? *cur-db-id *last-state]
  (au/go
    (loop [paths cur-paths]
      (let [[update* ch] (au/alts? [update-sub-ch unsub-ch])]
        (when-not @*stopped?
          (if (= unsub-ch ch)
            (ca/unsub updates-pub :all update-sub-ch)
            (let [{:keys [update-infos notify-all? db-id local-state]
                   :or {db-id @*cur-db-id
                        local-state @*local-state}} update*
                  update? (u/update-sub? update-infos paths)
                  paths* (if-not (or notify-all? update?)
                           paths
                           (let [si (au/<? (<make-si local-state db-id))
                                 new-state (:state si)
                                 new-paths (:paths si)]
                             (when (not= @*last-state new-state)
                               (reset! *last-state new-state)
                               (update-fn new-state))
                             new-paths))]
              (recur paths*))))))))

(defn subscribe!*
  [vc sub-map initial-state update-fn subscriber-name resolution-map
   sub-map->op-cache updates-pub log-error *local-state *stopped? *cur-db-id]
  (u/check-sub-map subscriber-name "subscriber" sub-map)
  (let [ordered-pairs (u/sub-map->ordered-pairs sub-map->op-cache sub-map)
        <make-si (partial u/<make-state-info vc ordered-pairs
                          subscriber-name resolution-map)
        update-sub-ch (ca/chan 10)
        unsub-ch (ca/promise-chan)
        unsubscribe! #(ca/put! unsub-ch true)]
    (ca/sub updates-pub :all update-sub-ch)
    (ca/go
      (try
        (au/<? (u/<wait-for-conn-init vc))
        (let [{cur-state :state
               cur-paths :paths} (au/<? (<make-si))
              *last-state (atom cur-state)]
          (when (not= initial-state cur-state)
            (update-fn cur-state))
          (au/<? (<subscription-loop update-fn cur-paths update-sub-ch unsub-ch
                                     updates-pub <make-si *local-state *stopped?
                                     *cur-db-id *last-state)))
        (catch #?(:cljs js/Error :clj Exception) e
          (log-error (str "Error in subscribe!\n"
                          (u/ex-msg-and-stacktrace e))))))
    unsubscribe!))

(defn handle-updates*
  [vc update-info cb* updates-ch log-error *cur-db-id *local-state]
  (ca/go
    (try
      (au/<? (u/<wait-for-conn-init vc))
      (let [{:keys [sys-cmds local-cmds]} update-info
            cb (or cb* (constantly nil))
            sys-ret (when (seq sys-cmds)
                      (when-let [ret (au/<? (u/<update-sys-state vc sys-cmds))]
                        (if (= :vivo/unauthorized ret)
                          ret
                          (let [{:keys [new-db-id prev-db-id update-infos]} ret
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
              (if (compare-and-set! *local-state cur-local-state local-state)
                (let [update-infos (concat (:update-infos sys-ret)
                                           (:update-infos local-ret))
                      {:keys [notify-all?]} sys-ret]
                  (ca/put! updates-ch (u/sym-map update-infos notify-all?
                                                 local-state))
                  (cb true))
                (if (< num-attempts max-commit-attempts)
                  (recur (inc num-attempts))
                  (cb (ex-info (str "Failed to commit updates after "
                                    num-attempts " attempts.")
                               (u/sym-map max-commit-attempts
                                          sys-cmds local-cmds)))))))))
      (catch #?(:cljs js/Error :clj Throwable) e
        (if cb*
          (cb* e)
          (log-error (u/ex-msg-and-stacktrace e)))))) )

(defrecord VivoClient [capsule-client sys-state-schema sys-state-source
                       log-info log-error rpcs
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
          (let [_ (au/<? (u/<wait-for-conn-init this))
                pcf (au/<? (cc/<send-msg capsule-client :get-schema-pcf fp))
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
          (au/<? (u/<wait-for-conn-init this))
          (au/<? (cc/<send-msg capsule-client :store-schema-pcf
                               (l/pcf schema))))
        fp)))

  u/IVivoClient
  (ssr? [this]
    (boolean @*ssr-info))

  (ssr-get-state! [this sub-map resolution-map]
    (ssr-get-state!* *ssr-info sub-map resolution-map))

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
               (if (empty? needed)
                 s
                 (do
                   (doseq [[sub-map resolution-map] needed]
                     (let [ret (au/<? (u/<make-state-info this sub-map
                                                          component-name
                                                          resolution-map))
                           stripped-rm (strip-non-vals resolution-map)]
                       (swap! *ssr-info update
                              :resolved assoc [sub-map stripped-rm]
                              (:state ret))))
                   (swap! *ssr-info assoc :needed #{})
                   (recur)))))
           (finally
             (reset! *ssr-info nil))))))

  (<deserialize-value [this path ret]
    (au/go
      (when ret
        (let [schema-path (rest path) ; Ignore :sys
              value-sch (u/path->schema path->schema-cache sys-state-schema
                                        schema-path)]
          (when value-sch
            (let [writer-sch (au/<? (u/<fp->schema this (:fp ret)))]
              (l/deserialize value-sch writer-sch (:bytes ret))))))))

  (<log-in! [this identifier secret]
    (au/go
      (when-not capsule-client
        (throw
         (ex-info (str "Can't log in because the :get-server-url "
                       "option was not provided when the vivo-client was "
                       "created.") {})))
      (u/check-secret-len secret)
      (sr/flush! sys-state-cache)
      (au/<? (u/<wait-for-conn-init this))
      (let [arg (u/sym-map identifier secret)
            ret (au/<? (cc/<send-msg capsule-client :log-in arg))
            {:keys [subject-id]} ret]
        (when subject-id
          (set-subject-id! subject-id))
        ret)))

  (<log-in-w-token! [this token]
    (when-not capsule-client
      (throw
       (ex-info (str "Can't log in because the :get-server-url "
                     "option was not provided when the vivo-client was "
                     "created.") {})))
    (au/go
      (if-let [subject-id (au/<? (cc/<send-msg capsule-client
                                               :log-in-w-token token))]
        (do
          (set-subject-id! subject-id)
          true)
        false)))

  (<log-out! [this]
    (au/go
      (set-subject-id! nil)
      (sr/flush! sys-state-cache)
      (let [ret (au/<? (cc/<send-msg capsule-client :log-out nil))]
        (log-info (str "Logout " (if ret "succeeded." "failed.")))
        ret)))

  (<log-out-w-token! [this token]
    (au/go
      (let [ret (au/<? (cc/<send-msg capsule-client :log-out-w-token token))]
        (log-info (str "Token logout " (if ret "succeeded." "failed."))))))

  (logged-in? [this]
    (boolean @*subject-id))

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
      ;; TODO: Optimize by doing <get-sys-state-and-expanded-path calls in
      ;;        parallel where possible (non-dependent)
      (let [init {:state resolution-map
                  :paths []}]
        (if-not (seq sub-map-or-ordered-pairs)
          init
          (let [ordered-pairs (if (map? sub-map-or-ordered-pairs)
                                (u/sub-map->ordered-pairs
                                 sub-map->op-cache
                                 sub-map-or-ordered-pairs)
                                sub-map-or-ordered-pairs)
                num-pairs (count ordered-pairs)
                sub-keys (map first ordered-pairs)]
            ;; Use loop instead of reduce here to stay within the go block
            (loop [acc init
                   i 0]
              (let [[sym path] (nth ordered-pairs i)
                    resolved-path (u/resolve-symbols-in-path
                                   acc subscriber-name path)
                    [path-head & path-tail] resolved-path
                    [v xp] (cond
                             (= [:vivo/subject-id] path)
                             [@*subject-id [[:vivo/subject-id]]]

                             (some nil? resolved-path)
                             [nil [resolved-path]]

                             (= :local path-head)
                             (au/<? (<get-state-and-expanded-path
                                     local-state resolved-path :local))

                             (= :sys path-head)
                             (when (and db-id (not @*stopped?))
                               (au/<? (u/<get-sys-state-and-expanded-path
                                       this db-id resolved-path))))
                    new-acc (-> acc
                                (assoc-in [:state sym] v)
                                (update :paths concat xp))
                    new-i (inc i)]
                (if (= num-pairs new-i)
                  (update new-acc :state select-keys sub-keys)
                  (recur new-acc new-i)))))))))

  (subscribe! [this sub-map initial-state update-fn subscriber-name
               resolution-map]
    (subscribe!* this sub-map initial-state update-fn subscriber-name
                 resolution-map sub-map->op-cache updates-pub log-error
                 *local-state *stopped? *cur-db-id))

  (<wait-for-conn-init [this]
    (au/go
      (loop [tries-remaining 600]
        (when (zero? tries-remaining)
          (throw (ex-info "Timed out waiting for connection to initialize."
                          {:cause :init-timeout})))
        (cond
          @*conn-initialized? true
          @*stopped? false
          :else (do
                  (ca/<! (ca/timeout 100))
                  (recur (dec tries-remaining)))))))

  (handle-updates [this update-info cb*]
    (handle-updates* this update-info cb* updates-ch log-error
                     *cur-db-id *local-state))

  (update-state! [this update-cmds cb]
    (when-not (sequential? update-cmds)
      (when cb
        (cb (ex-info "The update-cmds parameter must be a sequence."
                     (u/sym-map update-cmds)))))
    (let [update-info (make-update-info update-cmds)]
      (u/handle-updates this update-info cb))
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
            sucs-map (au/<? (ca/reduce (fn [acc v]
                                         (if (instance? #?(:cljs js/Error
                                                           :clj Throwable) v)
                                           (reduced v)
                                           (let [[i suc] v]
                                             (assoc acc i suc))))
                                       {} ch))
            _ (when (instance? #?(:cljs js/Error :clj Throwable) sucs-map)
                (throw sucs-map))
            sucs (keep sucs-map (range (count sys-cmds)))]
        (au/<? (cc/<send-msg capsule-client :update-state
                             sucs update-state-timeout-ms)))))

  (<update-cmd->serializable-update-cmd [this i cmd]
    (au/go
      (if-not (contains? cmd :arg)
        [i cmd]
        (let [{:keys [arg path]} cmd
              arg-sch (u/path->schema path->schema-cache sys-state-schema
                                      (rest path))]
          (if-not arg-sch
            [i nil]
            (let [fp (au/<? (u/<schema->fp this arg-sch))
                  bytes (l/serialize arg-sch (:arg cmd))
                  scmd (assoc cmd :arg (u/sym-map fp bytes))]
              (swap! *fp->schema assoc fp arg-sch)
              [i scmd]))))))

  (<get-sys-state-and-expanded-path [this db-id path]
    (when-not capsule-client
      (throw
       (ex-info (str "Can't get :sys state because the :get-server-url "
                     "option was not provided when the vivo-client was "
                     "created.") {})))
    (au/go
      (when db-id
        (or (sr/get sys-state-cache [db-id path])
            (when-not @*stopped?
              (let [arg (u/sym-map db-id path)
                    ret (au/<? (cc/<send-msg capsule-client :get-state arg
                                             get-state-timeout-ms))
                    [v xp] (if (#{nil
                                  :vivo/unauthorized
                                  :vivo/db-id-does-not-exist} ret)
                             [ret [path]]
                             (if-not ret
                               [nil [path]]
                               (let [v (au/<? (u/<deserialize-value
                                               this path
                                               (:serialized-value ret)))]
                                 [v (:expanded-path ret)])))]
                (when-not (= :vivo/db-id-does-not-exist)
                  (sr/put! sys-state-cache [db-id path] [v xp]))
                [v xp]))))))

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

  (<get-subject-id-for-identifier [this identifier]
    (cc/<send-msg capsule-client :get-subject-id-for-identifier identifier))

  (<add-subject! [this identifier secret]
    (u/<add-subject! this identifier secret) nil)

  (<add-subject! [this identifier secret subject-id]
    (au/go
      (u/check-secret-len secret)
      (au/<? (u/<wait-for-conn-init this))
      (au/<? (cc/<send-msg capsule-client :add-subject
                           (u/sym-map identifier secret subject-id)))))

  (<add-subject-identifier! [this identifier]
    (au/go
      (au/<? (u/<wait-for-conn-init this))
      (au/<? (cc/<send-msg capsule-client :add-subject-identifier identifier))))

  (<remove-subject-identifier! [this identifier]
    (au/go
      (au/<? (u/<wait-for-conn-init this))
      (au/<? (cc/<send-msg capsule-client :remove-subject-identifier
                           identifier))))

  (<change-secret! [this old-secret new-secret]
    (au/go
      (u/check-secret-len old-secret)
      (u/check-secret-len new-secret)
      (au/<? (cc/<send-msg capsule-client :change-secret
                           (u/sym-map old-secret new-secret)))))

  (<rpc [this rpc-name-kw arg timeout-ms]
    (au/go
      (when-not (keyword? rpc-name-kw)
        (throw (ex-info (str "rpc-name-kw must be a keyword. Got`" rpc-name-kw
                             "`.")
                        (u/sym-map rpc-name-kw arg))))
      (au/<? (u/<wait-for-conn-init this))
      (let [rpc-info (get rpcs rpc-name-kw)
            _ (when-not rpc-info
                (throw (ex-info
                        (str "No RPC with name `" rpc-name-kw "` is registered."
                             " Either this is a typo or you need to add `"
                             rpc-name-kw "` to the `:rpcs map "
                             "when creating the Vivo client.")
                        {:known-rpcs (keys rpcs)
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

(defn <on-connect
  [on-connect* sys-state-source log-error log-info *cur-db-id *conn-initialized?
   capsule-client]
  (ca/go
    (try
      (let [db-id (au/<? (cc/<send-msg capsule-client :set-state-source
                                       sys-state-source))]
        (reset! *cur-db-id db-id)
        (reset! *conn-initialized? true)
        (log-info "Vivo client connection initialized.")
        (on-connect*))
      (catch #?(:clj Exception :cljs js/Error) e
        (log-error (str "Error in <on-connect: "
                        (u/ex-msg-and-stacktrace e)))))))

(defn on-disconnect
  [on-disconnect* *conn-initialized? *cur-db-id set-subject-id! capsule-client]
  (on-disconnect*)
  (set-subject-id! nil)
  (reset! *conn-initialized? false)
  (reset! *cur-db-id nil))

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
  [get-server-url on-connect* on-disconnect* sys-state-schema sys-state-source
   log-error log-info *cur-db-id *conn-initialized? set-subject-id!]
  (when-not sys-state-schema
    (throw (ex-info (str "Missing `:sys-state-schema` option in vivo-client "
                         "constructor.")
                    {})))
  (let [get-credentials (constantly {:subject-id "vivo-client"
                                     :subject-secret ""})
        opts {:on-connect (partial <on-connect on-connect* sys-state-source
                                   log-error log-info *cur-db-id
                                   *conn-initialized?)
              :on-disconnect (partial on-disconnect on-disconnect*
                                      *conn-initialized? *cur-db-id
                                      set-subject-id!)}]
    (cc/client get-server-url get-credentials
               u/client-server-protocol :client opts)))

(defn vivo-client [opts]
  (let [{:keys [get-server-url
                initial-local-state
                log-error
                log-info
                on-connect
                on-disconnect
                rpcs
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
        *vc (atom nil)
        on-connect* #(when on-connect
                       (on-connect @*vc))
        on-disconnect* #(when on-disconnect
                          (on-disconnect @*vc @*local-state))
        updates-ch (ca/chan (ca/sliding-buffer 100))
        updates-pub (ca/pub updates-ch (constantly :all))
        set-subject-id! (fn [subject-id]
                          (reset! *subject-id subject-id)
                          (ca/put! updates-ch
                                   {:update-infos
                                    [{:norm-path [:vivo/subject-id]
                                      :op :set}]})
                          nil)
        path->schema-cache (sr/stockroom 100)
        sys-state-cache (sr/stockroom sys-state-cache-num-keys)
        sub-map->op-cache (sr/stockroom 500)
        _ (when rpcs
            (u/check-rpcs rpcs))
        capsule-client (when get-server-url
                         (check-sys-state-source sys-state-source)
                         (make-capsule-client
                          get-server-url on-connect* on-disconnect*
                          sys-state-schema sys-state-source
                          log-error log-info *cur-db-id *conn-initialized?
                          set-subject-id!))
        vc (->VivoClient capsule-client sys-state-schema sys-state-source
                         log-info log-error rpcs
                         sys-state-cache sub-map->op-cache
                         path->schema-cache updates-ch updates-pub
                         set-subject-id! *local-state *cur-db-id
                         *conn-initialized? *stopped? *ssr-info
                         *fp->schema *subject-id)]
    (reset! *vc vc)
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
