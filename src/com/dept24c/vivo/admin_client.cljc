(ns com.dept24c.vivo.admin-client
  (:require
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.client :as cc]
   [deercreeklabs.lancaster :as l]))

(def default-ac-opts
  {:log-error println
   :log-info println})

(defprotocol IAdminClient
  (<create-branch [this branch db-id])
  (<delete-branch [this branch])
  (shutdown! [this]))

(defrecord AdminClient [capsule-client log-error log-info]
  IAdminClient
  (<create-branch [this branch db-id]
    (cc/<send-msg capsule-client :create-branch (u/sym-map branch db-id)))

  (<delete-branch [this branch]
    (cc/<send-msg capsule-client :delete-branch branch))

  (shutdown! [this]
    (cc/shutdown capsule-client)
    (log-info "Vivo admin client stopped.")))

(defn admin-client
  ([get-server-url get-credentials]
   (admin-client get-server-url get-credentials {}))
  ([get-server-url get-credentials opts]
   (let [{:keys [log-error log-info]} (merge default-ac-opts opts)
         capsule-client (cc/client get-server-url get-credentials
                                   u/admin-client-server-protocol
                                   :admin-client)]
     (->AdminClient capsule-client log-error log-info))))
