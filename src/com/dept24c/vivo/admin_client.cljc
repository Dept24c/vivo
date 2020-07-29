(ns com.dept24c.vivo.admin-client
  (:require
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.client :as cc]
   [deercreeklabs.capsule.logging :as log]
   [deercreeklabs.lancaster :as l]))

(defprotocol IAdminClient
  (<create-branch [this branch db-id])
  (<delete-branch [this branch])
  (<get-db-id-for-branch [this branch])
  (shutdown! [this]))

(defrecord AdminClient [capsule-client]
  IAdminClient
  (<create-branch [this branch db-id]
    (cc/<send-msg capsule-client :create-branch (u/sym-map branch db-id)))

  (<delete-branch [this branch]
    (cc/<send-msg capsule-client :delete-branch branch))

  (<get-db-id-for-branch [this branch]
    (cc/<send-msg capsule-client :get-db-id-for-branch branch))

  (shutdown! [this]
    (cc/shutdown capsule-client)
    (log/info "Vivo admin client stopped.")))

(defn admin-client
  ([get-server-url get-credentials]
   (admin-client get-server-url get-credentials {}))
  ([get-server-url get-credentials opts]
   (let [capsule-client (cc/client get-server-url get-credentials
                                   u/admin-client-server-protocol
                                   :admin-client)]
     (->AdminClient capsule-client))))
