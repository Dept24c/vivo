(ns com.dept24c.vivo.server
  (:require
   [clojure.core.async :as ca]
   [clojure.string :as str]
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

(defn <handle-update-state [*conn-id->store-info arg metadata]
  (au/go
    (let [{:keys [tx-info-str update-cmds]} arg
          {:keys [conn-id]} metadata
          {:keys [branch schema-pcf]} (@*conn-id->store-info conn-id)]
      (when-not branch
        (throw-no-store-connected conn-id :update-state))

      )))

(defn vivo-server
  ([port]
   (vivo-server port {}))
  ([port state-schema opts]
   (let [{:keys [log-info log-error
                 handle-http http-timeout-ms]} (merge default-opts opts)
         protocol (u/make-sm-server-protocol state-schema)
         authenticator (constantly true)
         ep (ep/endpoint "state-manager" authenticator protocol :server)
         cs-opts (u/sym-map handle-http http-timeout-ms)
         capsule-server (cs/server [ep] port cs-opts)
         *conn-id->store-info (atom {})]
     (ep/set-handler ep :update-state
                     (partial <handle-update-state *conn-id->store-info))
     (cs/start capsule-server)
     (log-info (str "Vivo server started on port " port "."))
     #(cs/stop capsule-server))))
