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
   [deercreeklabs.lancaster.utils :as lu]
   [deercreeklabs.stockroom :as sr]))

(def default-send-msg-timeout-ms 30000)
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

(defn update-cmds-match? [ucs1 ucs2]
  (if (not= (count ucs1) (count ucs2))
    false
    (reduce (fn [acc i]
              (let [uc1 (nth ucs1 i)
                    uc2 (nth ucs2 i)]
                (if (= (dissoc uc1 :value) (dissoc uc2 :value))
                  acc
                  (reduced false))))
            true (range (count ucs1)))))

(defn do-sys-updates!
  [capsule-client sys-cmds <update-cmd->suc <update-sys-state!]
  (ca/go
    (try
      (when-not capsule-client
        (throw (ex-info (str "Can't update :sys state because the "
                             ":get-server-url option was not provided when the "
                             "vivo-client was created.")
                        {})))
      (let [ch (ca/merge (map-indexed <update-cmd->suc sys-cmds))
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
        (au/<? (<update-sys-state! sucs)))
      (catch #?(:cljs js/Error :clj Throwable) e
        (log/error (u/ex-msg-and-stacktrace e))))))

;; TODO: Handle failed sys update (offline data may obviate this need)
(defn do-state-updates!*
  [vc update-info cb* subscription-state-update-ch <fp->schema
   sys-state-schema subject-id *sys-db-info *local-state *state-sub-name->info ]
  (let [{:keys [sys-cmds local-cmds]} update-info
        cb (or cb* (constantly nil))
        sys-ret (when (seq sys-cmds)
                  (let [eval-ret (eval-cmds (:db @*sys-db-info)
                                            sys-cmds :sys)
                        {:keys [state update-infos]} eval-ret
                        <update-cmd->suc (partial
                                          u/<update-cmd->serializable-update-cmd
                                          vc)
                        <update-sys-state! #(u/<send-msg vc :update-state %)]
                    (do-sys-updates! (:capsule-client vc) sys-cmds
                                     <update-cmd->suc <update-sys-state!)
                    {:db state
                     :update-infos update-infos}))]
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
              (swap! *sys-db-info assoc :db db)
              (ca/put! subscription-state-update-ch
                       (u/sym-map db local-state subject-id
                                  update-infos cb *state-sub-name->info)))
            (if (< num-attempts max-commit-attempts)
              (recur (inc num-attempts))
              (cb (ex-info (str "Failed to commit updates after "
                                num-attempts " attempts.")
                           (u/sym-map max-commit-attempts
                                      sys-cmds local-cmds)))))))))

  nil)

(defn <wait-for-conn-init* [*stopped? *conn-initialized?]
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

(defn <do-login!
  [capsule-client identifier secret token wait-for-init? set-subject-id!
   *token *stopped? *conn-initialized?]
  (when secret
    (u/check-secret-len secret))
  (au/go
    (when wait-for-init?
      (au/<? (<wait-for-conn-init* *stopped? *conn-initialized?)))
    (let [[msg-name arg] (if identifier
                           [:log-in {:identifier identifier
                                     :secret secret}]
                           [:log-in-w-token token])
          ret (au/<? (cc/<send-msg capsule-client msg-name arg))
          {:keys [token subject-id]} ret]
      (if-not subject-id
        false
        (do
          (set-subject-id! subject-id)
          (reset! *token token)
          ret)))))

(defn <do-logout!
  [capsule-client token set-subject-id! *token]
  (au/go
    (reset! *token nil)
    (set-subject-id! nil) ; Do this explicitly in case we aren't connected
    (au/<? (if token
             (cc/<send-msg capsule-client :log-out-w-token token)
             (cc/<send-msg capsule-client :log-out nil)))
    ;; Don't need to request a :db-changed msg here because :log-out will
    ;; generate one if we are connected. If we aren't connected,
    ;; the request would fail anyway.
    ))

(defn <handle-db-changed*
  [arg metadata local-state local-db-id local-subject-id <fp->schema
   sys-state-schema subscription-state-update-ch *conn-initialized?
   *sys-db-info *state-sub-name->info *subject-id]
  (ca/go
    (try
      (let [{:keys [db-id prev-db-id serialized-state subject-id]} arg
            {:keys [fp bytes]} serialized-state
            writer-schema (when fp
                            (au/<? (<fp->schema fp)))
            missed-update? (and (not= local-db-id prev-db-id))
            subject-id-changed? (not= local-subject-id subject-id)
            update-infos (cond-> (:update-infos arg)
                           missed-update?
                           (conj {:norm-path [:sys]
                                  :op :set})

                           subject-id-changed?
                           (conj {:norm-path [:vivo/subject-id]
                                  :op :set}))
            db (when (and writer-schema bytes)
                 (l/deserialize sys-state-schema writer-schema bytes))
            cb (fn [_]
                 (reset! *conn-initialized? true))]
        (log/info (str "Got :db-changed msg.\n"
                       (u/pprint-str (u/sym-map db-id subject-id))))
        (swap! *sys-db-info (fn [old]
                              (cond-> (assoc old :db-id db-id)
                                ;; If db is nil, it means it didn't change
                                db (assoc :db db))))
        (when subject-id-changed?
          (reset! *subject-id subject-id))
        (ca/put! subscription-state-update-ch
                 (u/sym-map db local-state update-infos cb
                            *state-sub-name->info subject-id)))
      (catch #?(:cljs js/Error :clj Throwable) e
        (log/error (str "Exception in <handle-db-changed: "
                        (u/ex-msg-and-stacktrace e)))))))

(defrecord VivoClient [capsule-client
                       path->schema-cache
                       rpcs
                       set-subject-id!
                       subscription-state-update-ch
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
                       *token
                       *topic-name->sub-id->cb]
  u/ISchemaStore
  (<fp->schema [this fp]
    (when-not (int? fp)
      (throw (ex-info (str "Given `fp` arg is not a `long`. Got `"
                           (or fp "nil") "`.")
                      {:given-fp fp})))
    (au/go
      (or (@*fp->schema fp)
          (if-let [schema (some-> (cc/<send-msg capsule-client
                                                :get-schema-pcf fp)
                                  (au/<?)
                                  (l/json->schema))]
            (do
              (swap! *fp->schema assoc fp schema)
              schema)
            (throw (ex-info
                    (str "Failed to get a schema for fp `" fp "`.")
                    (u/sym-map fp)))))))

  (<schema->fp [this schema]
    (when-not (l/schema? schema)
      (throw (ex-info (str "Schema arg must be a Lancaster schema. Got `"
                           schema "`.")
                      {:given-schema schema})))
    (au/go
      (let [fp (l/fingerprint64 schema)]
        (when-not (@*fp->schema fp)
          (swap! *fp->schema assoc fp schema)
          (au/<? (cc/<send-msg capsule-client
                               :store-schema-pcf (l/pcf schema))))
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
    (<do-login! capsule-client identifier secret nil true set-subject-id! *token
                *stopped? *conn-initialized?))

  (<log-in-w-token! [this token]
    (<do-login! capsule-client nil nil token true set-subject-id! *token
                *stopped? *conn-initialized?))

  (<log-out! [this]
    (<do-logout! capsule-client nil set-subject-id! *token))

  (<log-out-w-token! [this token]
    (<do-logout! capsule-client token set-subject-id! *token))

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
     state-sub-name sub-map update-fn opts @*sys-db-info @*local-state
     @*subject-id *stopped? *state-sub-name->info))

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
    (<wait-for-conn-init* *stopped? *conn-initialized?))

  (update-state! [this update-cmds cb]
    (when-not (sequential? update-cmds)
      (when cb
        (cb (ex-info "The update-cmds parameter must be a sequence."
                     (u/sym-map update-cmds)))))
    (let [update-info (make-update-info update-cmds)]
      (do-state-updates!* this update-info cb subscription-state-update-ch
                          #(u/<fp->schema this %) sys-state-schema
                          @*subject-id *sys-db-info *local-state
                          *state-sub-name->info))
    nil)


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

  (<handle-db-changed [this arg metadata]
    (<handle-db-changed* arg metadata @*local-state (:db-id @*sys-db-info)
                         @*subject-id #(u/<fp->schema this %) sys-state-schema
                         subscription-state-update-ch *conn-initialized?
                         *sys-db-info *state-sub-name->info *subject-id))

  (<send-msg [this msg-name msg]
    (u/<send-msg this msg-name msg default-send-msg-timeout-ms))

  (<send-msg [this msg-name msg timeout-ms]
    (au/go
      (when-not capsule-client
        (throw
         (ex-info (str "Can't perform network operation because the "
                       ":get-server-url option was not provided when the "
                       "vivo-client was created.") {})))
      (au/<? (u/<wait-for-conn-init this))
      (au/<? (cc/<send-msg capsule-client msg-name msg timeout-ms))))

  (<get-subject-id-for-identifier [this identifier]
    (u/<send-msg this :get-subject-id-for-identifier identifier))

  (<add-subject! [this identifier secret]
    (u/<add-subject! this identifier secret nil))

  (<add-subject! [this identifier secret subject-id]
    (u/check-secret-len secret)
    (u/<send-msg this :add-subject (u/sym-map identifier secret subject-id)))

  (<add-subject-identifier! [this identifier]
    (u/<send-msg this :add-subject-identifier identifier))

  (<remove-subject-identifier! [this identifier]
    (u/<send-msg this :remove-subject-identifier identifier))

  (<change-secret! [this old-secret new-secret]
    (u/check-secret-len old-secret)
    (u/check-secret-len new-secret)
    (u/<send-msg this :change-secret (u/sym-map old-secret new-secret)))

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
            ret* (au/<? (u/<send-msg this :rpc arg timeout-ms))]
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
  [opts-on-connect sys-state-source set-subject-id! *conn-initialized? *vc
   *stopped? *token capsule-client]
  (when-not @*stopped?
    (ca/go
      (try
        (au/<? (cc/<send-msg capsule-client :set-state-source sys-state-source))
        (let [vc @*vc]
          ;; Either of these generate a :request-db-changed-msg.
          (if-let [token @*token]
            (au/<? (<do-login! capsule-client nil nil token false
                               set-subject-id! *token *stopped?
                               *conn-initialized?))
            (au/<? (cc/<send-msg capsule-client :request-db-changed-msg nil)))
          (au/<? (u/<wait-for-conn-init vc))
          (when opts-on-connect
            (let [ret (opts-on-connect vc)]
              (when (au/channel? ret)
                (au/<? ret)))) ;; Check for errors
          (log/info "Vivo client connection initialized."))
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
   sys-state-source *sys-db-info *vc *conn-initialized? *stopped? *token
   set-subject-id!]
  (when-not sys-state-schema
    (throw (ex-info (str "Missing `:sys-state-schema` option in vivo-client "
                         "constructor.")
                    {})))
  (let [get-credentials (constantly {:subject-id "vivo-client"
                                     :subject-secret ""})
        opts {:on-connect (partial <on-connect opts-on-connect sys-state-source
                                   set-subject-id! *conn-initialized? *vc
                                   *stopped? *token)
              :on-disconnect (partial on-disconnect opts-on-disconnect
                                      *conn-initialized? set-subject-id!)}]
    (cc/client get-server-url get-credentials
               u/client-server-protocol :client opts)))

(defn set-handlers! [vc capsule-client *fp->schema]
  (cc/set-handler capsule-client :db-changed
                  (partial u/<handle-db-changed vc))
  (cc/set-handler capsule-client :get-schema-pcf
                  (fn [fp metadata]
                    (if-let [schema (@*fp->schema fp)]
                      (l/pcf schema)
                      (do
                        (log/error
                         (str "Could not find PCF for fingerprint `"
                              fp "`."))
                        nil)))))

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
        *token (atom nil)
        *topic-name->sub-id->cb (atom {})
        *next-instance-num (atom 0)
        *next-topic-sub-id (atom 0)
        on-disconnect* #(when on-disconnect
                          (on-disconnect @*vc @*local-state))
        ;; TODO: Think about this buffer size and dropping behavior under load
        ;; Perhaps disconnect and reconnect later if overloaded?
        subscription-state-update-ch (ca/chan (ca/sliding-buffer 1000))
        set-subject-id! (fn [subject-id]
                          (reset! *subject-id subject-id)
                          (let [update-infos [{:norm-path [:vivo/subject-id]
                                               :op :set}]
                                db (:db @*sys-db-info)
                                local-state @*local-state]
                            (ca/put! subscription-state-update-ch
                                     (u/sym-map db local-state update-infos
                                                subject-id
                                                *state-sub-name->info)))
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
                          *token set-subject-id!))
        vc (->VivoClient capsule-client
                         path->schema-cache
                         rpcs
                         set-subject-id!
                         subscription-state-update-ch
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
                         *token
                         *topic-name->sub-id->cb)]
    (reset! *vc vc)
    (state-subscriptions/start-subscription-update-loop!
     subscription-state-update-ch)
    (when get-server-url
      (set-handlers! vc capsule-client *fp->schema))
    vc))
