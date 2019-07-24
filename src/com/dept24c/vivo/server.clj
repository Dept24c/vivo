(ns com.dept24c.vivo.server
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [com.dept24c.bristlecone :as bc]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.baracus :as ba]
   [deercreeklabs.capsule.endpoint :as ep]
   [deercreeklabs.capsule.server :as cs]
   [deercreeklabs.lancaster :as l])
  (:import
   (clojure.lang ExceptionInfo)
   (java.security SecureRandom)))

(defn default-health-http-handler [req]
  (if (= "/health" (:uri req))
    {:status 200
     :headers {"content-type" "text/plain"
               "Access-Control-Allow-Origin" "*"}
     :body "I am healthy"}
    {:status 404
     :body "I still haven't found what you're looking for..."}))

(def default-opts
  {:authentication-fn (fn [identifier secret]
                        nil) ;; return subject-id or nil
   :authorization-fn (fn [subject-id path]
                       false) ;; return true or false
   :handle-http default-health-http-handler
   :http-timeout-ms 60000
   :log-info println
   :log-error println ;; TODO: use stderr
   :login-lifetime-mins (* 60 24 30)}) ;; 30 days

(defn start-state-update-loop
  [ep ch bc-client authorization-fn tx-fns log-error]
  (ca/go-loop []
    (try
      (let [{:keys [subject-id branch update-cmds conn-ids ret-ch]} (au/<? ch)
            authorized? (reduce (fn [acc {:keys [path] :as cmd}]
                                  (if (authorization-fn subject-id path)
                                    acc
                                    (reduced false)))
                                true update-cmds)]
        (if-not authorized?
          (ca/>! ret-ch false)
          (let [ret (au/<? (bc/<commit! bc-client branch update-cmds ""
                                        tx-fns))
                change-info (assoc ret :updated-paths (map :path update-cmds))]
            (doseq [conn-id conn-ids]
              (ep/send-msg ep conn-id :sys-state-changed change-info))
            (ca/>! ret-ch change-info))))
      (catch Exception e
        (log-error (str "Error in state-update-loop:\n"
                        (u/ex-msg-and-stacktrace e)))))
    (recur)))

(defn <get-temp-branch [bc-client source]
  (au/go
    (let [db-id (:temp-branch/db-id source)
          branch (str "temp-branch-" (rand-int 1e9))]
      (au/<? (bc/<create-branch! bc-client branch db-id true))
      branch)))

(defn <handle-update-state
  [ep bc-client authorization-fn tx-fns log-error *conn-id->info
   *branch->info arg metadata]
  (let [{:keys [conn-id]} metadata
        {:keys [subject-id branch]} (@*conn-id->info conn-id)
        xf-cmd #(update % :arg u/value-rec->edn)
        update-cmds (map xf-cmd (:update-cmds arg))
        {:keys [update-ch]
         branch-conn-ids :conn-ids} (@*branch->info branch)
        conn-ids (disj branch-conn-ids conn-id)
        ret-ch (ca/chan)
        ch (or update-ch
               (let [ch* (ca/chan 100)]
                 (swap! *branch->info assoc-in [branch :update-ch] ch*)
                 (start-state-update-loop ep ch* bc-client authorization-fn
                                          tx-fns log-error)
                 ch*))]
    (ca/put! ch (u/sym-map subject-id branch update-cmds conn-ids ret-ch))
    ret-ch))

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
          db-id (au/<? (bc/<get-db-id bc-client branch))]
      (or db-id
          (let [default-data (l/default-data sys-state-schema)
                update-cmds [{:path []
                              :op :set
                              :arg default-data}]
                ret (au/<? (bc/<commit! bc-client branch update-cmds
                                        "Create initial db"))]
            (:cur-db-id ret))))))

(defn <handle-get-state
  [bc-client state-schema authorization-fn *conn-id->info arg metadata]
  (au/go
    (let [{:keys [conn-id]} metadata
          {:keys [subject-id branch]} (@*conn-id->info conn-id)
          {:keys [path db-id]} arg
          authorized? (let [ret (authorization-fn subject-id path)]
                        (if (au/channel? ret)
                          (au/<? ret)
                          ret))]
      (if-not authorized?
        {:is-forbidden true}
        {:v (->> (bc/<get-in bc-client db-id path)
                 (au/<?)
                 (u/edn->value-rec state-schema path))}))))

(defn generate-token []
  (let [rng (SecureRandom.)
        bytes (byte-array 32)]
    (.nextBytes rng bytes)
    (ba/byte-array->b64 bytes)))

(defn <store-token-info
  [bc-client table-name subject-id token token-expiration-time-mins]
  (au/go
    #_
    (let [arg {:op :PutItem
               :request {:TableName table-name
                         :Item {"token" {:S token}
                                "subject-id" {:S subject-id}
                                "token-expiration-time-mins"
                                {:N (str token-expiration-time-mins)}}}}
          ret (au/<? (aws-async/invoke ddb-client arg))]
      (= {} ret))))

(defn <batch-store-token-infos [bc-client table-name infos]
  (au/go
    #_
    (let [->rq (fn [{:keys [token subject-id token-expiration-time-mins]}]
                 {:PutRequest {:Item {"token" {:S token}
                                      "subject-id" {:S subject-id}
                                      "token-expiration-time-mins"
                                      {:N (str token-expiration-time-mins)}}}})
          arg {:op :BatchWriteItem
               :request {:RequestItems {table-name (map ->rq infos)}
                         :ReturnConsumedCapacity "TOTAL"
                         :ReturnItemCollectionMetrics "SIZE"}}
          ret (au/<? (aws-async/invoke ddb-client arg))]
      ret)))

(defn mapval [m]
  (-> m first second))

(defn token-info-ddb->clj
  [{:keys [token subject-id token-expiration-time-mins]}]
  {:token (mapval token)
   :subject-id (mapval subject-id)
   :token-expiration-time-mins
   (u/str->int (mapval token-expiration-time-mins))})

(defn <get-token-info [bc-client table-name subject-id]
  (au/go
    #_
    (let [arg {:op :GetItem
               :request {:TableName table-name
                         :Key {"subject-id" {:S subject-id}}}}
          ret (au/<? (aws-async/invoke ddb-client arg))]
      (some-> ret :Item token-info-ddb->clj))))

(defn <get-all-token-infos [bc-client table-name]
  (au/go
    #_
    (loop [out []
           last-key nil]
      (let [arg {:op :Scan
                 :request (cond-> {:TableName table-name}
                            last-key (assoc :ExclusiveStartKey last-key))}
            ret (au/<? (aws-async/invoke ddb-client arg))
            infos (some->> ret :Items (map token-info-ddb->clj))
            new-out (into out infos)]
        (if-let [new-last-key (:LastEvaluatedKey ret)]
          (recur new-out new-last-key)
          new-out)))))

(defn <delete-token-info [bc-client subject-id]
  (au/go
    ;; TODO: Implement
    ))

(defn <handle-token-log-in [bc-client table-name subject-id submitted-token]
  (au/go
    #_
    (let [{:keys [token]} (au/<? (<get-token-info ddb-client table-name
                                                  subject-id))]
      (when (= token submitted-token)
        (u/sym-map subject-id token)))))

(defn <handle-normal-log-in
  [bc-client table-name authentication-fn identifier secret token-life-mins]
  (au/go
    (let [subject-id (let [ret (authentication-fn identifier secret)]
                       (if (au/channel? ret)
                         (au/<? ret)
                         ret))]
      (when subject-id
        (let [token (generate-token)
              token-expiration-time-mins (+ (u/ms->mins (u/current-time-ms))
                                            token-life-mins)]
          (au/<? (<store-token-info bc-client table-name subject-id token
                                    token-expiration-time-mins))
          (u/sym-map subject-id token))))))

(defn <handle-log-in [ep bc-client authentication-fn token-life-mins
                      *conn-id->info *subject-id->conn-ids arg metadata]
  (au/go
    #_
    (let [{:keys [identifier secret is-token]} arg
          {:keys [conn-id]} metadata
          ret (au/<? (if is-token
                       (<handle-token-log-in bc-client table-name
                                             identifier secret)
                       (<handle-normal-log-in bc-client table-name
                                              authentication-fn identifier
                                              secret token-life-mins)))
          {:keys [subject-id]} ret]
      (when ret
        (swap! *conn-id->info update conn-id assoc :subject-id subject-id)
        (swap! *subject-id->conn-ids update subject-id
               (fn [old-conn-ids]
                 (if old-conn-ids
                   (conj old-conn-ids conn-id)
                   #{conn-id})))
        ret))))

(defn <log-out-subject-id*
  [ep bc-client subject-id log-error *subject-id->conn-ids]
  (au/go
    (au/<? (<delete-token-info bc-client subject-id))
    (doseq [conn-id (@*subject-id->conn-ids subject-id)]
      (ep/close-conn ep conn-id))
    (swap! *subject-id->conn-ids dissoc subject-id)
    true))

(defn <handle-log-out
  [ep bc-client log-error *subject-id->conn-ids *conn-id->info arg metadata]
  (let [{:keys [conn-id]} metadata
        {:keys [subject-id]} (@*conn-id->info conn-id)]
    (<log-out-subject-id* ep bc-client subject-id log-error
                          *subject-id->conn-ids *conn-id->info)))

(defn start-token-expiration-loop
  [ep bc-client log-error *subject-id->conn-ids]
  ;; (ca/go-loop []
  ;;   (try
  ;;     (doseq [info (au/<? (<get-expired-token-infos bc-client))]
  ;;       (au/<? (<log-out-subject-id* ep bc-client (:subject-id info)
  ;;                                    log-error *subject-id->conn-ids)))
  ;;     (ca/<! (ca/timeout 60000)) ;; 1 minute
  ;;     (catch Exception e
  ;;       (log-error (str "Error in token-expriration-loop:\n"
  ;;                       (u/ex-msg-and-stacktrace e)))))
  ;;   (recur))
  )

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
          (au/<? (bc/<delete-branch! bc-client branch)))
        (log-info "Client disconnected (conn-info: " conn-info ")."))
      (catch Throwable e
        (log-error (str "Error in on-disconnect (conn-info: " conn-info ")\n"
                        (u/ex-msg-and-stacktrace e)))))))

(defn vivo-server
  "Returns a no-arg fn that stops the server."
  ([port repository-name sys-state-schema]
   (vivo-server port repository-name sys-state-schema {}))
  ([port repository-name sys-state-schema opts]
   (let [{:keys [authentication-fn
                 authorization-fn
                 handle-http
                 http-timeout-ms
                 log-error
                 log-info
                 login-lifetime-mins
                 transaction-fns]} (merge default-opts opts)
         bc-client (au/<?? (bc/<bristlecone-client
                            repository-name sys-state-schema))
         protocol (u/make-sm-server-protocol sys-state-schema)
         authenticator (constantly true)
         *conn-id->info (atom {})
         *subject-id->conn-ids (atom {})
         *branch->info (atom {})
         ep-opts {:on-disconnect (partial on-disconnect *conn-id->info
                                          *subject-id->conn-ids *branch->info
                                          bc-client log-error log-info)}
         ep (ep/endpoint "state-manager" authenticator protocol :server ep-opts)
         cs-opts (u/sym-map handle-http http-timeout-ms)
         capsule-server (cs/server [ep] port cs-opts)]
     (start-token-expiration-loop ep bc-client log-error *subject-id->conn-ids)
     (ep/set-handler ep :log-in
                     (partial <handle-log-in ep bc-client
                              authentication-fn login-lifetime-mins
                              *conn-id->info *subject-id->conn-ids))
     (ep/set-handler ep :log-out
                     (partial <handle-log-out ep bc-client log-error
                              *subject-id->conn-ids *conn-id->info))
     (ep/set-handler ep :set-state-source
                     (partial <handle-set-state-source
                              *conn-id->info *branch->info bc-client
                              sys-state-schema))
     (ep/set-handler ep :get-state
                     (partial <handle-get-state bc-client sys-state-schema
                              authorization-fn *conn-id->info))
     (ep/set-handler ep :update-state
                     (partial <handle-update-state ep bc-client
                              authorization-fn transaction-fns log-error
                              *conn-id->info *branch->info))
     (cs/start capsule-server)
     (log-info (str "Vivo server started on port " port "."))
     #(cs/stop capsule-server))))
