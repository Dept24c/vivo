(ns com.dept24c.vivo.client
  (:require
   [clojure.core.async :as ca]
   [com.dept24c.vivo.bristlecone.block-ids :as block-ids]
   [com.dept24c.vivo.client.topic-subscriptions :as topic-subscriptions]
   [com.dept24c.vivo.client.state-subscriptions :as state-subscriptions]
   [com.dept24c.vivo.commands :as commands]
   [com.dept24c.vivo.react :as react]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.client :as cc]
   [deercreeklabs.capsule.logging :as log]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.stockroom :as sr]))

(def get-state-timeout-ms 30000)
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

(defn do-state-updates!*
  [vc update-info msg-info cb* state-update-ch <fp->schema sys-state-schema
   *sys-db-info *local-state *state-sub-name->info *subject-id]
  (ca/go
    (try
      (au/<? (u/<wait-for-conn-init vc))
      (let [{:keys [sys-cmds local-cmds]} update-info
            cb (or cb* (constantly nil))
            sys-ret (when (seq sys-cmds)
                      (when-let [ret (au/<? (u/<update-sys-state vc sys-cmds))]
                        (if (= :vivo/unauthorized ret)
                          ret
                          (let [{:keys [new-db-id
                                        new-serialized-db update-infos]} ret
                                {:keys [fp bytes]} new-serialized-db
                                writer-schema (au/<? (<fp->schema fp))
                                db (l/deserialize sys-state-schema
                                                  writer-schema bytes)
                                local-db-id (:db-id @*sys-db-info)]
                            (log/info (str "Updated state. New db-id: "
                                           new-db-id))
                            (reset! *sys-db-info {:db-id new-db-id
                                                  :db db})
                            (u/sym-map update-infos db)))))]
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
                      db (or (:db sys-ret)
                             (:db @*sys-db-info))]
                  (ca/put! state-update-ch
                           (u/sym-map db local-state update-infos cb
                                      msg-info *state-sub-name->info
                                      *subject-id)))
                (if (< num-attempts max-commit-attempts)
                  (recur (inc num-attempts))
                  (cb (ex-info (str "Failed to commit updates after "
                                    num-attempts " attempts.")
                               (u/sym-map max-commit-attempts
                                          sys-cmds local-cmds)))))))))
      (catch #?(:cljs js/Error :clj Throwable) e
        (log/error (u/ex-msg-and-stacktrace e))
        (when cb*
          (cb* e)))))
  nil)

(defrecord VivoClient [capsule-client
                       path->schema-cache
                       rpcs
                       set-subject-id!
                       state-update-ch
                       sys-state-schema
                       sys-state-source
                       *conn-initialized?
                       *fp->schema
                       *local-state
                       *next-instance-num
                       *next-topic-sub-id
                       *state-sub-name->info
                       *stopped?
                       *subject-id
                       *sys-db-info
                       *topic-name->sub-id->cb]
  u/ISchemaStore
  (<fp->schema [this fp]
    (when-not (int? fp)
      (throw (ex-info (str "Given `fp` arg is not a `long`. Got `"
                           (or fp "nil") "`.")
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
  (next-instance-num! [this]
    (swap! *next-instance-num inc))

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
      (au/<? (u/<wait-for-conn-init this))
      (let [arg (u/sym-map identifier secret)
            ret (au/<? (cc/<send-msg capsule-client :log-in arg))
            {:keys [subject-id token was-successful]} ret]
        (set-subject-id! subject-id)
        (if was-successful
          (u/sym-map subject-id token)
          false))))

  (<log-in-w-token! [this token]
    (when-not capsule-client
      (throw
       (ex-info (str "Can't log in because the :get-server-url "
                     "option was not provided when the vivo-client was "
                     "created.") {})))
    (au/go
      (let [subject-id (au/<? (cc/<send-msg capsule-client
                                            :log-in-w-token token))]
        (set-subject-id! subject-id)
        (if subject-id
          (u/sym-map subject-id token)
          false))))

  (<log-out! [this]
    (au/go
      (set-subject-id! nil)
      (au/<? (cc/<send-msg capsule-client :log-out nil))))

  (<log-out-w-token! [this token]
    (cc/<send-msg capsule-client :log-out-w-token token))

  (logged-in? [this]
    (boolean @*subject-id))

  (shutdown! [this]
    (reset! *stopped? true)
    (cc/shutdown capsule-client)
    (log/info "Vivo client stopped."))

  (get-subscription-info [this state-sub-name]
    (when-let [info (@*state-sub-name->info state-sub-name)]
      (select-keys info [:state :resolution-map])))

  (subscribe-to-state! [this state-sub-name sub-map update-fn opts]
    (state-subscriptions/subscribe-to-state!
     state-sub-name sub-map update-fn opts sys-state-source
     *stopped? *state-sub-name->info *sys-db-info *local-state *subject-id))

  (unsubscribe-from-state! [this state-sub-name]
    (swap! *state-sub-name->info dissoc state-sub-name)
    nil)

  (subscribe-to-topic! [this scope topic-name cb]
    (topic-subscriptions/subscribe-to-topic!
     scope topic-name cb *next-topic-sub-id *topic-name->sub-id->cb))

  (publish-to-topic! [this scope topic-name msg]
    (topic-subscriptions/publish-to-topic!
     scope topic-name msg *topic-name->sub-id->cb))

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

  (update-state! [this update-cmds cb]
    (when-not (sequential? update-cmds)
      (when cb
        (cb (ex-info "The update-cmds parameter must be a sequence."
                     (u/sym-map update-cmds)))))
    (let [update-info (make-update-info update-cmds)]
      (do-state-updates!* this update-info nil cb state-update-ch
                          #(u/<fp->schema this %) sys-state-schema
                          *sys-db-info *local-state
                          *state-sub-name->info *subject-id))
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

  (<handle-sys-state-changed [this arg metadata]
    (ca/go
      (try
        (let [{:keys [new-db-id new-serialized-db update-infos]} arg
              {:keys [fp bytes]} new-serialized-db
              writer-schema (when fp
                              (au/<? (u/<fp->schema this fp)))
              local-db-id (:db-id @*sys-db-info)
              db (when (and writer-schema bytes)
                   (l/deserialize sys-state-schema writer-schema bytes))
              local-state @*local-state]
          (log/info (str "Got :sys-state-changed msg. New db-id: " new-db-id))
          (reset! *sys-db-info {:db-id new-db-id
                                :db db})
          (ca/put! state-update-ch (u/sym-map db local-state update-infos
                                              *state-sub-name->info
                                              *subject-id)))
        (catch #?(:cljs js/Error :clj Throwable) e
          (log/error (str "Exception in <handle-sys-state-changed: "
                          (u/ex-msg-and-stacktrace e)))))))

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
  [opts-on-connect sys-state-source *conn-initialized? *sys-db-info *vc
   *stopped? capsule-client]
  (when-not @*stopped?
    (ca/go
      (try
        (let [db-info (au/<? (cc/<send-msg capsule-client :set-state-source
                                           sys-state-source))]
          (if (= :vivo/unauthorized db-info)
            (throw (ex-info (str "Failed to set state source. Got `"
                                 ":vivo/unauthorized`.")
                            (u/sym-map db-info)))
            (let [vc @*vc]
              (reset! *conn-initialized? true)
              (when-not (= (:db-id db-info) (:db-id @*sys-db-info))
                (au/<? (u/<handle-sys-state-changed
                        vc
                        {:new-db-id (:db-id db-info)
                         :new-serialized-db (:serialized-db db-info)
                         :update-infos [{:norm-path [:sys]
                                         :op :set}]}
                        {})))
              (when opts-on-connect
                (opts-on-connect vc))
              (log/info "Vivo client connection initialized."))))
        (catch #?(:clj Exception :cljs js/Error) e
          (log/error (str "Error in <on-connect: "
                          (u/ex-msg-and-stacktrace e))))))))

(defn on-disconnect
  [on-disconnect* *conn-initialized? set-subject-id! capsule-client]
  (reset! *conn-initialized? false)
  (on-disconnect*))

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
  [get-server-url opts-on-connect opts-on-disconnect sys-state-schema
   sys-state-source *sys-db-info *vc *conn-initialized? *stopped?
   set-subject-id!]
  (when-not sys-state-schema
    (throw (ex-info (str "Missing `:sys-state-schema` option in vivo-client "
                         "constructor.")
                    {})))
  (let [get-credentials (constantly {:subject-id "vivo-client"
                                     :subject-secret ""})
        opts {:on-connect (partial <on-connect opts-on-connect sys-state-source
                                   *conn-initialized? *sys-db-info *vc
                                   *stopped?)
              :on-disconnect (partial on-disconnect opts-on-disconnect
                                      *conn-initialized? set-subject-id!)}]
    (cc/client get-server-url get-credentials
               u/client-server-protocol :client opts)))

(defn vivo-client [opts]
  (let [{:keys [get-server-url
                initial-local-state
                on-connect
                on-disconnect
                rpcs
                sys-state-source
                sys-state-schema]} opts
        *local-state (atom initial-local-state)
        *sys-db-info (atom {:db-id nil
                            :db nil})
        *conn-initialized? (atom (not get-server-url))
        *stopped? (atom false)
        *fp->schema (atom {})
        *subject-id (atom nil)
        *vc (atom nil)
        *state-sub-name->info (atom {})
        *topic-name->sub-id->cb (atom {})
        *next-instance-num (atom 0)
        *next-topic-sub-id (atom 0)
        on-disconnect* #(when on-disconnect
                          (on-disconnect @*vc @*local-state))
        ;; TODO: Think about this buffer size and dropping behavior under load
        ;; Perhaps disconnect and reconnect later if overloaded?
        state-update-ch (ca/chan (ca/sliding-buffer 1000))
        set-subject-id! (fn [subject-id]
                          (reset! *subject-id subject-id)
                          (let [update-infos [{:norm-path [:vivo/subject-id]
                                               :op :set}]
                                db (:db @*sys-db-info)
                                local-state @*local-state]
                            (ca/put! state-update-ch
                                     (u/sym-map db local-state update-infos
                                                *state-sub-name->info
                                                *subject-id)))
                          nil)
        _ (when rpcs
            (u/check-rpcs rpcs))
        path->schema-cache (sr/stockroom 1000)
        capsule-client (when get-server-url
                         (check-sys-state-source sys-state-source)
                         (make-capsule-client
                          get-server-url on-connect on-disconnect*
                          sys-state-schema sys-state-source
                          *sys-db-info *vc *conn-initialized? *stopped?
                          set-subject-id!))
        vc (->VivoClient capsule-client
                         path->schema-cache
                         rpcs
                         set-subject-id!
                         state-update-ch
                         sys-state-schema
                         sys-state-source
                         *conn-initialized?
                         *fp->schema
                         *local-state
                         *next-instance-num
                         *next-topic-sub-id
                         *state-sub-name->info
                         *stopped?
                         *subject-id
                         *sys-db-info
                         *topic-name->sub-id->cb)]
    (reset! *vc vc)
    (state-subscriptions/start-subscription-update-loop! state-update-ch)
    (when get-server-url
      (cc/set-handler capsule-client :sys-state-changed
                      (partial u/<handle-sys-state-changed vc))
      (cc/set-handler capsule-client :get-schema-pcf
                      (fn [fp metadata]
                        (if-let [schema (@*fp->schema fp)]
                          (l/pcf schema)
                          (do
                            (log/error
                             (str "Could not find PCF for fingerprint `"
                                  fp "`."))
                            nil)))))
    vc))
