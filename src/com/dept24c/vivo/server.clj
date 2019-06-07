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
  {:handle-http default-health-http-handler
   :http-timeout-ms 60000
   :log-info println
   :log-error println}) ;; TODO: use stderr

(defn throw-no-store-connected [conn-id msg-name]
  (throw (ex-info (str "Store has not been connected for conn-id `"
                       conn-id "`.")
                  {:type :no-store-connected
                   :conn-id conn-id
                   :msg-name msg-name})))

(defn <handle-get-state
  [state-schema *conn-id->info *db-id->state *db-id-num arg metadata]
  (au/go
    (let [{:keys [path]} arg
          {:keys [conn-id]} metadata
          {:keys [br-client subject-id]} (@*conn-id->info conn-id)
          _ (when-not br-client
              (throw-no-store-connected conn-id :get-state))
          db-id (or (:db-id arg)
                    ;; TODO: Get the latest db-id from the selected branch
                    (u/long->b62 @*db-id-num))
          state (@*db-id->state db-id)
          value (->> (state/get-in-state state path)
                     (:val)
                     (u/edn->value-rec state-schema path))]
      (u/sym-map db-id value))))

(defn <handle-update-state
  [ep state-schema *conn-id->info *db-id->state *db-id-num arg metadata]
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
          new-state (reduce state/eval-cmd old-state update-cmds)
          change-info (u/sym-map db-id tx-info-str)]
      (swap! *db-id->state assoc db-id new-state)
      (ep/send-msg-to-all-conns ep :store-changed change-info)
      change-info)))

(defn handle-connect-store [state-schema *conn-id->info arg metadata]
  (let [{:keys [conn-id]} metadata
        {:keys [store-name branch]} arg
        {:keys [subject-id]} (@*conn-id->info conn-id)
        br-client (br/bristlecone-client store-name state-schema subject-id)]
    (swap! *conn-id->info assoc conn-id (u/sym-map br-client subject-id))
    true))

(defn handle-login-subject [*conn-id->info arg metadata]
  (let [{:keys [subject-id secret]} arg
        {:keys [conn-id]} metadata]
    ;; TODO: Implement
    (println "@@@@@ in handle-login-subject")
    false))

(defn vivo-server
  ([port state-schema]
   (vivo-server port state-schema {}))
  ([port state-schema opts]
   (let [{:keys [log-info log-error initial-sys-state
                 handle-http http-timeout-ms]} (merge default-opts opts)
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
     (ep/set-handler ep :connect-store
                     (partial handle-connect-store state-schema *conn-id->info))
     (ep/set-handler ep :get-state
                     (partial <handle-get-state state-schema *conn-id->info
                              *db-id->state *db-id-num))
     (ep/set-handler ep :login-subject
                     (partial handle-login-subject *conn-id->info))
     (ep/set-handler ep :update-state
                     (partial <handle-update-state ep state-schema *conn-id->info
                              *db-id->state *db-id-num))
     (cs/start capsule-server)
     (log-info (str "Vivo server started on port " port "."))
     #(cs/stop capsule-server))))
