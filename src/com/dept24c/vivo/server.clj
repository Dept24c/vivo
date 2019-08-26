(ns com.dept24c.vivo.server
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [cognitect.aws.client.api :as aws]
   [cognitect.aws.client.api.async :as aws-async]
   [com.dept24c.vivo.bristlecone.client :as bcc]
   [com.dept24c.vivo.utils :as u]
   [crypto.password.bcrypt :as bcrypt]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.capsule.endpoint :as ep]
   [deercreeklabs.capsule.server :as cs]
   [deercreeklabs.lancaster :as l]
   [deercreeklabs.stockroom :as sr])
  (:import
   (clojure.lang ExceptionInfo)
   (java.security SecureRandom)
   (java.util UUID)))

(def creds-key-root "--VIVO-CREDS-")
(def idents-key-root "--VIVO-IDENTS-")
(def login-tokens-key "--VIVO-LOGIN-TOKENS")
(def work-factor 12)

(defn default-health-http-handler [req]
  (if (= "/health" (:uri req))
    {:status 200
     :headers {"content-type" "text/plain"
               "Access-Control-Allow-Origin" "*"}
     :body "I am healthy"}
    {:status 404
     :body "I still haven't found what you're looking for..."}))

(def default-opts
  {:additional-endpoints []
   :authorization-fn (fn [subject-id path]
                       false) ;; return true or false
   :handle-http default-health-http-handler
   :http-timeout-ms 60000
   :log-info println
   :log-error println ;; TODO: use stderr
   :login-lifetime-mins (* 60 24 15)}) ;; 15 days

(defn start-state-update-loop
  [ep ch bc-client authorization-fn tx-fns log-error]
  (ca/go-loop []
    (let [{:keys [subject-id branch update-cmds conn-ids ret-ch]} (au/<? ch)]
      (try
        (let [authorized? (reduce (fn [acc {:keys [path] :as cmd}]
                                    (if (authorization-fn subject-id path)
                                      acc
                                      (reduced false)))
                                  true update-cmds)]
          (if-not authorized?
            (ca/>! ret-ch false)
            (let [ret (au/<? (u/<commit! bc-client branch update-cmds ""
                                         tx-fns))
                  change-info (assoc ret :updated-paths
                                     (map :path update-cmds))]
              (doseq [conn-id conn-ids]
                (ep/send-msg ep conn-id :sys-state-changed change-info))
              (ca/>! ret-ch change-info))))
        (catch Exception e
          (log-error (str "Error in state-update-loop:\n"
                          (u/ex-msg-and-stacktrace e)))
          (ca/>! ret-ch e))))
    (recur)))

(defn <get-temp-branch [bc-client source]
  (au/go
    (let [db-id (:temp-branch/db-id source)
          branch (str "temp-branch-" (rand-int 1e9))]
      (au/<? (u/<create-branch! bc-client branch db-id true))
      branch)))

(defn <fp->schema [ep conn-id *fp->schema fp]
  (au/go
    (or (@*fp->schema fp)
        (let [pcf (au/<? (ep/<send-msg ep conn-id :get-schema-pcf fp))
              schema (l/json->schema pcf)]
          (swap! *fp->schema assoc fp schema)
          schema))))

(defn <xf-update-cmds
  [ep conn-id state-schema path->schema-cache *fp->schema scmds]
  (au/go
    (if-not (seq scmds)
      []
      ;; Use loop/recur to stay in single go block
      (loop [scmd (first scmds)
             i 0
             out []]
        (let [{:keys [path arg]} scmd
              arg-sch (when arg
                        (or (sr/get path->schema-cache path)
                            (let [sch (l/schema-at-path state-schema path)]
                              (sr/put! path->schema-cache path sch)
                              sch)))
              writer-arg-sch (when arg
                               (au/<? (<fp->schema ep conn-id *fp->schema
                                                   (:fp arg))))
              cmd (update scmd :arg (fn [arg]
                                      (when arg
                                        (l/deserialize arg-sch writer-arg-sch
                                                       (:bytes arg)))))
              new-i (inc i)
              new-out (conj out cmd)]
          (if (> (count scmds) new-i)
            (recur (nth scmds new-i) new-i new-out)
            new-out))))))

(defn <handle-update-state
  [ep bc-client state-schema authorization-fn tx-fns log-error
   path->schema-cache *conn-id->info *branch->info *fp->schema arg metadata]
  (au/go
    (let [{:keys [conn-id]} metadata
          {:keys [subject-id branch]} (@*conn-id->info conn-id)
          {:keys [update-ch]
           branch-conn-ids :conn-ids} (@*branch->info branch)
          ret-ch (ca/chan)
          conn-ids (disj branch-conn-ids conn-id)
          update-cmds (au/<? (<xf-update-cmds ep conn-id state-schema
                                              path->schema-cache *fp->schema
                                              arg))
          ch (or update-ch
                 (let [ch* (ca/chan 100)]
                   (swap! *branch->info assoc-in [branch :update-ch] ch*)
                   (start-state-update-loop ep ch* bc-client authorization-fn
                                            tx-fns log-error)
                   ch*))]
      (ca/>! ch (u/sym-map subject-id branch update-cmds conn-ids ret-ch))
      (au/<? ret-ch))))

(defn handle-get-schema-pcf [log-error *fp->schema fp metadata]
  (au/go
    (if-let [schema (@*fp->schema fp)]
      (l/pcf schema)
      (do
        (log-error (str "Could not find PCF for fingerprint `" fp "`."))
        nil))))

(defn <handle-set-state-source
  [*conn-id->info *branch->info bc-client sys-state-schema source metadata]
  (au/go
    (let [{:keys [conn-id]} metadata
          perm-branch (:branch source)
          branch (or perm-branch
                     (au/<? (<get-temp-branch bc-client source)))
          _ (swap! *conn-id->info update conn-id assoc
                   :branch branch :temp-branch? (not perm-branch))
          _ (swap! *branch->info update branch
                   (fn [{:keys [conn-ids] :as info}]
                     (if conn-ids
                       (update info :conn-ids conj conn-id)
                       (assoc info :conn-ids #{conn-id}))))
          db-id (au/<? (u/<get-db-id bc-client branch))]
      (or db-id
          (let [default-data (l/default-data sys-state-schema)
                update-cmds [{:path []
                              :op :set
                              :arg default-data}]
                ret (au/<? (u/<commit! bc-client branch update-cmds
                                       "Create initial db" []))]
            (:cur-db-id ret))))))

(defn <handle-get-state
  [bc-client state-schema authorization-fn path->schema-cache *conn-id->info
   *fp->schema arg metadata]
  (au/go
    (let [{:keys [conn-id]} metadata
          {:keys [subject-id branch]} (@*conn-id->info conn-id)
          {:keys [path db-id]} arg
          authorized? (let [ret (authorization-fn subject-id path)]
                        (if (au/channel? ret)
                          (au/<? ret)
                          ret))]
      (if-not authorized?
        :vivo/unauthorized
        (when-let [v (au/<? (u/<get-in bc-client db-id path))]
          (let [schema (or (sr/get path->schema-cache path)
                           (let [sch (l/schema-at-path state-schema path)]
                             (sr/put! path->schema-cache path sch)
                             sch))
                fp (l/fingerprint64 schema)]
            (swap! *fp->schema assoc fp schema)
            {:fp fp
             :bytes (l/serialize schema v)}))))))

(defn generate-token []
  (let [rng (SecureRandom.)
        bytes (byte-array 32)]
    (.nextBytes rng bytes)
    (ba/byte-array->b64 bytes)))

(defn <get-ddb-item [ddb-client table-name k]
  (au/go
    (let [arg {:op :GetItem
               :request {:TableName table-name
                         :Key {"k" {:S k}}}}
          ret (au/<? (aws-async/invoke ddb-client arg))]
      (some-> ret :Item))))

(defn <store-ddb-item [ddb-client table-name item]
  (au/go
    (let [arg {:op :PutItem
               :request {:TableName table-name
                         :Item item}}
          ret (au/<? (aws-async/invoke ddb-client arg))]
      (or (= {} ret)
          (throw (ex-info "<store-ddb-item failed."
                          (u/sym-map arg ret)))))))

(defn <delete-ddb-item [ddb-client table-name k]
  (au/go
    (let [arg {:op :DeleteItem
               :request {:TableName table-name
                         :Key {"k" {:S k}}}}
          ret (au/<? (aws-async/invoke ddb-client arg))]
      (or (= {} ret)
          (throw (ex-info "<delete-ddb-item failed."
                          (u/sym-map arg ret)))))))

(defn <get-v [ddb-client table-name k schema]
  (au/go
    (some->> (<get-ddb-item ddb-client table-name k)
             (au/<?)
             (:v)
             (:B)
             (slurp)
             (ba/b64->byte-array)
             ;; TODO: Use reader and writer schema
             (l/deserialize-same schema))))

;; TODO: Replace before 1,000 users
(defn <get-token-map [ddb-client table-name]
  (<get-v ddb-client table-name login-tokens-key u/token-map-schema))

;; TODO: Replace before 1,000 users
(defn <store-token-map [ddb-client table-name m]
  (let [v (->> (l/serialize u/token-map-schema m)
               (ba/byte-array->b64))
        item {"k" {:S login-tokens-key}
              "v" {:B v}}]
    (<store-ddb-item ddb-client table-name item)))

(defn <handle-token-log-in [ep bc-client ddb-client table-name *conn-id->info
                            *subject-id->conn-ids token metadata]
  (au/go
    (when-let [subject-id (some-> (<get-token-map ddb-client table-name)
                                  (au/<?)
                                  (get token)
                                  (:subject-id))]
      (let [{:keys [conn-id]} metadata]
        (swap! *conn-id->info update conn-id assoc :subject-id subject-id)
        (swap! *subject-id->conn-ids update subject-id
               (fn [conn-ids]
                 (conj (or conn-ids #{}) conn-id)))
        subject-id))))

(defn <handle-log-in
  [ep bc-client ddb-client table-name token-lifetime-mins log-error
   *conn-id->info *subject-id->conn-ids arg metadata]
  (ca/go
    (try
      (let [{:keys [conn-id]} metadata
            {:keys [identifier secret]} arg
            k (str idents-key-root identifier)
            item (au/<? (<get-ddb-item ddb-client table-name k))
            subject-id (some-> item :subject-id :S)]
        (when subject-id
          (let [creds-k (str creds-key-root subject-id)
                subject-info (au/<? (<get-v ddb-client table-name creds-k
                                            u/subject-info-schema))]
            (when (bcrypt/check secret (:hashed-secret subject-info))
              (let [token (generate-token)
                    expiration-time-mins (+ (u/ms->mins (u/current-time-ms))
                                            token-lifetime-mins)
                    token-info (u/sym-map expiration-time-mins subject-id)
                    token-map (au/<? (<get-token-map ddb-client table-name))
                    new-token-map (assoc (or token-map {}) token token-info)]
                (au/<? (<store-token-map ddb-client table-name new-token-map))
                (swap! *conn-id->info update conn-id
                       assoc :subject-id subject-id)
                (swap! *subject-id->conn-ids update subject-id
                       (fn [conn-ids]
                         (conj (or conn-ids #{}) conn-id)))
                (u/sym-map subject-id token))))))
      (catch Exception e
        (log-error (str "Error in <handle-log-in:\n"
                        (u/ex-msg-and-stacktrace e)))))))

(defn <handle-log-out
  [ep bc-client ddb-client table-name log-error *subject-id->conn-ids
   *conn-id->info arg metadata]
  (ca/go
    (try
      (let [{:keys [conn-id]} metadata
            {:keys [subject-id]} (@*conn-id->info conn-id)
            token-map (au/<? (<get-token-map ddb-client table-name))
            new-token-map (reduce-kv
                           (fn [acc token info]
                             (if (= subject-id (:subject-id info))
                               acc
                               (assoc acc token info)))
                           {} token-map)
            conn-ids (@*subject-id->conn-ids subject-id)]
        (au/<? (<store-token-map ddb-client table-name new-token-map))
        (doseq [conn-id conn-ids]
          (ep/close-conn ep conn-id)))
      (catch Exception e
        (log-error (str "Error in <handle-log-out:\n"
                        (u/ex-msg-and-stacktrace e)))))))

(defn <add-subject [ddb-client table-name identifiers secret]
  (when-not (sequential? identifiers)
    (throw (ex-info (str "identifiers must be a sequence. Got: `"
                         (or identifiers "nil") "`.")
                    (u/sym-map identifiers secret))))
  (when-not (every? string? identifiers)
    (throw (ex-info (str "identifiers must all be strings. Got: `"
                         identifiers  "`.")
                    (u/sym-map identifiers secret))))
  (au/go
    (let [subject-id (.toString ^UUID (UUID/randomUUID))
          hashed-secret (bcrypt/encrypt secret work-factor)
          subject-info (u/sym-map hashed-secret identifiers)
          si-bytes (l/serialize u/subject-info-schema subject-info)
          creds-item {"k" {:S (str creds-key-root subject-id)}
                      "v" {:B (ba/byte-array->b64 si-bytes)}}]
      (doseq [identifier identifiers]
        (let [k (str idents-key-root identifier)
              existing (au/<? (<get-ddb-item ddb-client table-name k))
              id-item {"k" {:S k}
                       "subject-id" {:S subject-id}}]
          (when existing
            (throw (ex-info (str "identifier `" identifier
                                 "` already exists.")
                            (u/sym-map identifier identifiers))))
          (au/<? (<store-ddb-item ddb-client table-name id-item))))
      (au/<? (<store-ddb-item ddb-client table-name creds-item))
      subject-id)))

(defn <delete-subject [ddb-client table-name subject-id]
  (when-not (string? subject-id)
    (throw (ex-info (str "subject-id must be a string. Got: `"
                         (or subject-id "nil") "`.")
                    (u/sym-map subject-id))))
  (au/go
    (let [creds-k (str creds-key-root subject-id)
          ids (-> (<get-v ddb-client table-name creds-k u/subject-info-schema)
                  (au/<?)
                  (:identifiers))]
      (au/<? (<delete-ddb-item ddb-client table-name creds-k))
      (doseq [identifier ids]
        (au/<? (<delete-ddb-item ddb-client table-name
                                 (str idents-key-root identifier))))
      true)))

(defn <add-subject-identifier [ddb-client table-name subject-id identifier]
  (when-not (string? subject-id)
    (throw (ex-info (str "subject-id must be a string. Got: `"
                         (or subject-id "nil") "`.")
                    (u/sym-map subject-id))))
  (when-not string? identifier
            (throw (ex-info (str "identifier must be a string. Got: `"
                                 identifier  "`.")
                            (u/sym-map identifier subject-id))))
  (au/go
    (let [creds-k (str creds-key-root subject-id)
          subject-info (au/<? (<get-v ddb-client table-name creds-k
                                      u/subject-info-schema))
          add-id #(-> (set %)
                      (conj identifier)
                      (seq))
          si-bytes (l/serialize u/subject-info-schema
                                (update subject-info :identifiers add-id))
          creds-item {"k" {:S creds-k}
                      "v" {:B (ba/byte-array->b64 si-bytes)}}
          id-k (str idents-key-root identifier)
          existing (au/<? (<get-ddb-item ddb-client table-name id-k))
          id-item {"k" {:S id-k}
                   "subject-id" {:S subject-id}}]
      (when existing
        (throw (ex-info (str "identifier `" identifier
                             "` already exists.")
                        (u/sym-map identifier))))
      (au/<? (<store-ddb-item ddb-client table-name id-item))
      (au/<? (<store-ddb-item ddb-client table-name creds-item))
      true)))

(defn <delete-subject-identifier [ddb-client table-name subject-id identifier]
  (when-not (string? subject-id)
    (throw (ex-info (str "subject-id must be a string. Got: `"
                         (or subject-id "nil") "`.")
                    (u/sym-map identifier subject-id))))
  (when-not string? identifier
            (throw (ex-info (str "identifier must be a string. Got: `"
                                 identifier  "`.")
                            (u/sym-map identifier subject-id))))
  (au/go
    (let [creds-k (str creds-key-root subject-id)
          subject-info (au/<? (<get-v ddb-client table-name creds-k
                                      u/subject-info-schema))
          rm-id #(-> (set %)
                     (disj identifier)
                     (seq))
          si-bytes (l/serialize u/subject-info-schema
                                (update subject-info :identifiers rm-id))
          creds-item {"k" {:S creds-k}
                      "v" {:B (ba/byte-array->b64 si-bytes)}}]
      (au/<? (<delete-ddb-item ddb-client table-name
                               (str idents-key-root identifier)))
      (au/<? (<store-ddb-item ddb-client table-name creds-item))
      true)))

(defn <change-secret [ddb-client table-name subject-id secret]
  (when-not (string? subject-id)
    (throw (ex-info (str "subject-id must be a string. Got: `"
                         (or subject-id "nil") "`.")
                    (u/sym-map subject-id))))
  (when-not string? secret
            (throw (ex-info (str "secret must be a string. Got: `"
                                 (or secret "nil") "`.")
                            (u/sym-map subject-id))))
  (au/go
    (let [creds-k (str creds-key-root subject-id)
          new-hash (bcrypt/encrypt secret work-factor)
          subject-info (-> (<get-v ddb-client table-name creds-k
                                   u/subject-info-schema)
                           (au/<?)
                           (assoc :hashed-secret new-hash))
          si-bytes (l/serialize u/subject-info-schema
                                subject-info)
          creds-item {"k" {:S creds-k}
                      "v" {:B (ba/byte-array->b64 si-bytes)}}]
      (au/<? (<store-ddb-item ddb-client table-name creds-item))
      true)))

(defn start-token-expiration-loop
  [ep bc-client ddb-client table-name log-error *subject-id->conn-ids]
  (ca/go-loop []
    (try
      (let [token-map (au/<? (<get-token-map ddb-client table-name))
            now-mins (u/ms->mins (u/current-time-ms))
            m (reduce-kv
               (fn [acc token info]
                 (if (>= now-mins (:expiration-time-mins info))
                   (update acc :expired-subject-ids conj
                           (:subject-id info))
                   (update acc :new-token-map assoc token info)))
               {:new-token-map {}
                :expired-subject-ids #{}}
               token-map)
            {:keys [new-token-map expired-subject-ids]} m]
        (when (seq expired-subject-ids)
          (au/<? (<store-token-map ddb-client table-name new-token-map))
          (doseq [subject-id expired-subject-ids]
            (doseq [conn-id (@*subject-id->conn-ids subject-id)]
              (ep/close-conn ep conn-id))))
        (ca/<! (ca/timeout 60000))) ;; 1 minute
      (catch Exception e
        (log-error (str "Error in token-expriration-loop:\n"
                        (u/ex-msg-and-stacktrace e)))))
    (recur)))

(defn on-disconnect
  [*conn-id->info *subject-id->conn-ids *branch->info bc-client
   log-error log-info {:keys [conn-id] :as conn-info}]
  (ca/go
    (try
      (let [{:keys [branch subject-id temp-branch?]} (@*conn-id->info conn-id)]
        (swap! *conn-id->info dissoc conn-id)
        (swap! *branch->info update branch update :conn-ids disj conn-id)
        (swap! *subject-id->conn-ids
               (fn [m]
                 (let [new-conn-ids (disj (m subject-id) conn-id)]
                   (if (seq new-conn-ids)
                     (assoc m subject-id new-conn-ids)
                     (dissoc m subject-id)))))
        (when temp-branch?
          (au/<? (u/<delete-branch! bc-client branch)))
        (log-info "Client disconnected (conn-info: " conn-info ")."))
      (catch Throwable e
        (log-error (str "Error in on-disconnect (conn-info: " conn-info ")\n"
                        (u/ex-msg-and-stacktrace e)))))))

(defn vivo-server
  "Returns a no-arg fn that stops the server."
  ([port repository-name state-schema]
   (vivo-server port repository-name state-schema {}))
  ([port repository-name state-schema opts]
   (let [{:keys [additional-endpoints
                 authorization-fn
                 handle-http
                 http-timeout-ms
                 log-error
                 log-info
                 login-lifetime-mins
                 transaction-fns]} (merge default-opts opts)
         bc-client (au/<?? (bcc/<bristlecone-client
                            repository-name state-schema))
         *conn-id->info (atom {})
         *subject-id->conn-ids (atom {})
         *branch->info (atom {})
         *fp->schema (atom {})
         path->schema-cache (sr/stockroom 1000)
         ep-opts {:on-disconnect (partial on-disconnect *conn-id->info
                                          *subject-id->conn-ids *branch->info
                                          bc-client log-error log-info)}
         ep (ep/endpoint "state-manager" (constantly true) u/sm-server-protocol
                         :server ep-opts)
         cs-opts (u/sym-map handle-http http-timeout-ms)
         capsule-server (cs/server (conj additional-endpoints ep) port cs-opts)
         ddb-client (aws/client {:api :dynamodb})]
     (start-token-expiration-loop ep bc-client ddb-client repository-name
                                  log-error *subject-id->conn-ids)
     (ep/set-handler ep :log-in
                     (partial <handle-log-in ep bc-client ddb-client
                              repository-name login-lifetime-mins log-error
                              *conn-id->info *subject-id->conn-ids))
     (ep/set-handler ep :log-in-w-token
                     (partial <handle-token-log-in ep bc-client ddb-client
                              repository-name *conn-id->info
                              *subject-id->conn-ids))
     (ep/set-handler ep :log-out
                     (partial <handle-log-out ep bc-client ddb-client
                              repository-name log-error *subject-id->conn-ids
                              *conn-id->info))
     (ep/set-handler ep :set-state-source
                     (partial <handle-set-state-source
                              *conn-id->info *branch->info bc-client
                              state-schema))
     (ep/set-handler ep :get-state
                     (partial <handle-get-state bc-client state-schema
                              authorization-fn path->schema-cache
                              *conn-id->info *fp->schema))
     (ep/set-handler ep :update-state
                     (partial <handle-update-state ep bc-client state-schema
                              authorization-fn transaction-fns log-error
                              path->schema-cache *conn-id->info *branch->info
                              *fp->schema))
     (ep/set-handler ep :get-schema-pcf
                     (partial handle-get-schema-pcf log-error *fp->schema))
     (cs/start capsule-server)
     (log-info (str "Vivo server started on port " port "."))
     #(cs/stop capsule-server))))
