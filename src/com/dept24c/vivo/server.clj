(ns com.dept24c.vivo.server
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
   [com.dept24c.vivo.bristlecone :as br]
   [com.dept24c.vivo.state :as state]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.endpoint :as ep]
   [deercreeklabs.capsule.server :as cs]
   [deercreeklabs.lancaster :as l])
  (:import
   (clojure.lang ExceptionInfo)))

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
   :log-error println}) ;; TODO: use stderr

(defn throw-no-store-connected [conn-id msg-name]
  (throw (ex-info (str "Store has not been connected for conn-id `"
                       conn-id "`.")
                  {:type :no-store-connected
                   :conn-id conn-id
                   :msg-name msg-name})))

(defn handle-connect-store [state-schema *conn-id->info arg metadata]
  (let [{:keys [conn-id]} metadata
        {:keys [store-name branch]} arg
        conn-info (@*conn-id->info conn-id)
        {:vivo/keys [subject-id]} conn-info
        br-client (br/bristlecone-client store-name state-schema subject-id)]
    (swap! *conn-id->info update conn-id assoc :br-client br-client)
    true))

(defn <handle-get-state
  [state-schema authorization-fn *conn-id->info *db-id->state *db-id-num
   arg metadata]
  (au/go
    (let [{:keys [path db-id]} arg
          {:keys [conn-id]} metadata
          {:keys [br-client subject-id]} (@*conn-id->info conn-id)]
      (when-not br-client
        (throw-no-store-connected conn-id :get-state))
      (if-not (authorization-fn subject-id path)
        {:db-id db-id
         :is-unauthorized true}
        (let [db-id* (or db-id
                         ;; TODO: Get the latest db-id from the selected branch
                         (u/long->b62 @*db-id-num))
              state (@*db-id->state db-id*)
              value (->> (state/get-in-state state path)
                         (:val)
                         (u/edn->value-rec state-schema path))]
          {:db-id db-id*
           :value value})))))

(defn <handle-update-state
  [ep state-schema authorization-fn *conn-id->info *db-id->state *db-id-num
   arg metadata]
  (au/go
    (let [{:keys [tx-info-str]} arg
          {:keys [conn-id]} metadata
          {:keys [br-client subject-id]} (@*conn-id->info conn-id)
          _ (when-not br-client
              (throw-no-store-connected conn-id :update-state))
          xf-cmd (fn [{:keys [path] :as cmd}]
                   (update cmd :arg #(u/value-rec->edn state-schema path %)))
          update-cmds (map xf-cmd (:update-cmds arg))
          ;; TODO: Get the latest db-id from the selected branch
          ;; Handle CAS
          old-db-id (u/long->b62 @*db-id-num)
          db-id (u/long->b62 (swap! *db-id-num inc))
          old-state (@*db-id->state old-db-id)
          new-state (reduce (fn [acc {:keys [path] :as cmd}]
                              (if (authorization-fn subject-id path)
                                (state/eval-cmd acc cmd)
                                (reduced :vivo/unauthorized)))
                            old-state update-cmds)]
      (if (= :vivo/unauthorized new-state)
        false
        (let [change-info (u/sym-map db-id tx-info-str)]
          (swap! *db-id->state assoc db-id new-state)
          (ep/send-msg-to-all-conns ep :store-changed change-info)
          true)))))

(defn <handle-log-in [ep authentication-fn *conn-id->info arg metadata]
  (au/go
    (let [{:keys [identifier secret]} arg
          {:keys [conn-id]} metadata
          subject-id (authentication-fn identifier secret)]
      (when subject-id
        (swap! *conn-id->info update conn-id assoc :subject-id subject-id)
        (ep/send-msg ep conn-id :subject-id-changed subject-id))
      (boolean subject-id))))

(defn <handle-log-out [ep *conn-id->info arg metadata]
  (au/go
    (let [{:keys [conn-id]} metadata
          subject-id nil]
      (swap! *conn-id->info update conn-id assoc :subject-id subject-id)
      (ep/send-msg ep conn-id :subject-id-changed subject-id)
      true)))

(defn vivo-server
  ([port state-schema]
   (vivo-server port state-schema {}))
  ([port state-schema opts]
   (let [{:keys [log-info log-error initial-sys-state
                 handle-http http-timeout-ms
                 authentication-fn authorization-fn]} (merge default-opts opts)
         protocol (u/make-sm-server-protocol state-schema)
         authenticator (constantly true)
         ep (ep/endpoint "state-manager" authenticator protocol :server)
         cs-opts (u/sym-map handle-http http-timeout-ms)
         capsule-server (cs/server [ep] port cs-opts)
         *conn-id->info (atom {})

         ;; TODO: Remove these when bristlecone v1 is implemented
         initial-db-id-num 0
         *db-id-num (atom initial-db-id-num)
         db-id (u/long->b62 initial-db-id-num)
         *db-id->state (atom {db-id initial-sys-state})]
     (ep/set-handler ep :log-in
                     (partial <handle-log-in ep authentication-fn
                              *conn-id->info))
     (ep/set-handler ep :log-out
                     (partial <handle-log-out ep *conn-id->info))
     (ep/set-handler ep :connect-store
                     (partial handle-connect-store state-schema *conn-id->info))
     (ep/set-handler ep :get-state
                     (partial <handle-get-state state-schema authorization-fn
                              *conn-id->info *db-id->state *db-id-num))
     (ep/set-handler ep :update-state
                     (partial <handle-update-state ep state-schema
                              authorization-fn
                              *conn-id->info *db-id->state *db-id-num))
     (cs/start capsule-server)
     (log-info (str "Vivo server started on port " port "."))
     #(cs/stop capsule-server))))
