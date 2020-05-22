(ns com.dept24c.vivo
  (:require
   [clojure.core.async :as ca]
   [com.dept24c.vivo.client :as client]
   #?(:clj [com.dept24c.vivo.server :as server])
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]))

;;;;;;;;;;;;;;;;;;;; Server fns ;;;;;;;;;;;;;;;;;;;;

#?(:clj
   (defn vivo-server
     "Returns a no-arg fn that stops the server."
     [config]
     (server/vivo-server config)))

#?(:clj
   (defn set-rpc-handler! [server rpc-name-kw handler]
     (server/set-rpc-handler! server rpc-name-kw handler)))

#?(:clj
   (defn shutdown-server! [server]
     (server/shutdown! server)))

;;;;;;;;;;;;;;;;;;;; Client fns ;;;;;;;;;;;;;;;;;;;;

(defn vivo-client
  "Creates a Vivo client."
  ([]
   (vivo-client {}))
  ([opts]
   (client/vivo-client opts)))

(defn subscribe!
  "Creates a Vivo subscription. When any of the paths in the `sub-map`
   change, calls `update-fn` with the updated state. Note that this
   is a low-level function that generally should not be called directly.
   Prefer `react/def-component` or `react/use-vivo-state`.
   `opts` is a map of optional parameters:
     - `parents`: sequence of sub-names
     - `react?`: boolean. Enables react update batching
     - `resolution-map`: map of symbols to values to be used w/ sub-map
   Returns the current state if it is immediately available, else :vivo/unknown.
   Use `unsubscribe!` to cancel the subscription."
  ([vc sub-name sub-map update-fn]
   (subscribe! vc sub-name sub-map update-fn {}))
  ([vc sub-name sub-map update-fn opts]
   (u/subscribe! vc sub-name sub-map update-fn opts)))

(defn unsubscribe!
  "Cancels a subscription"
  [vc sub-name]
  (u/unsubscribe! vc sub-name))

(defn update-state!
  ([vc update-commands]
   (update-state! vc update-commands nil))
  ([vc update-commands cb]
   (u/update-state! vc update-commands cb)
   nil))

(defn <update-state!
  ([vc update-commands]
   (let [ch (ca/chan)
         cb #(ca/put! ch %)]
     (u/update-state! vc update-commands cb)
     ch)))

(defn set-state!
  ([vc path arg]
   (set-state! vc path arg nil))
  ([vc path arg cb]
   (u/update-state! vc [{:path path
                         :op :set
                         :arg arg}] cb)))

(defn <set-state!
  ([vc path arg]
   (let [ch (ca/chan)
         cb #(ca/put! ch %)]
     (set-state! vc path arg cb)
     ch)))

(defn logged-in? [vc]
  (u/logged-in? vc))

(defn <log-in!
  [vc identifier secret]
  (u/<log-in! vc identifier secret))

(defn <log-in-w-token!
  [vc token]
  (u/<log-in-w-token! vc token))

(defn <log-out!
  "Log out from the Vivo server."
  [vc]
  (u/<log-out! vc))

(defn <log-out-w-token!
  "Log out a user from the Vivo server using their token"
  [vc token]
  (u/<log-out-w-token! vc token))

(defn <add-subject!
  ([vc identifier secret]
   (<add-subject! vc identifier secret nil))
  ([vc identifier secret subject-id]
   (u/<add-subject! vc identifier secret subject-id)))

(defn <add-subject-identifier! [vc identifier]
  (u/<add-subject-identifier! vc identifier))

(defn <remove-subject-identifier! [vc identifier]
  (u/<remove-subject-identifier! vc identifier))

(defn <get-subject-id-for-identifier [vc identifier]
  (u/<get-subject-id-for-identifier vc identifier))

(defn <change-secret!
  [vc old-secret new-secret]
  (u/<change-secret! vc old-secret new-secret))

(defn <schema->fp
  "Get the fingerprint for the given schema and durably store the schema
   for future use by <fp->schema.
   Returns a channel which will yield the fingerprint."
  [vc schema]
  (u/<schema->fp vc schema))

(defn <fp->schema
  "Get the schema for the given fingerprint.
   Returns a channel which will yield the schema or nil if the fingerprint
   is unknown."
  [vc fp]
  (u/<fp->schema vc fp))

(defn default-rpc-timeout-ms 30000)

(defn rpc
  "Calls a remote procedure on the server. Calls callback `cb` with result."
  ([vc rpc-name-kw arg]
   (rpc vc rpc-name-kw arg default-rpc-timeout-ms nil))
  ([vc rpc-name-kw arg timeout-ms]
   (rpc vc rpc-name-kw arg timeout-ms nil))
  ([vc rpc-name-kw arg timeout-ms cb]
   (ca/go
     (try
       (let [ret (au/<? (u/<rpc vc rpc-name-kw arg timeout-ms))]
         (when cb
           (cb ret)))
       (catch #?(:cljs js/Error :clj Throwable) e
         (when-let [logger (:log-error vc)]
           (logger (str "Exception in rpc:\n" (u/ex-msg-and-stacktrace e))))
         (when cb
           (cb e)))))
   nil))

(defn <rpc
  "Calls a remote procedure on the server. Returns a channel which will yield
   the result."
  ([vc rpc-name-kw arg]
   (<rpc vc rpc-name-kw arg default-rpc-timeout-ms))
  ([vc rpc-name-kw arg timeout-ms]
   (u/<rpc vc rpc-name-kw arg timeout-ms)))

(defn shutdown!
  "Shutdown the vivo client and its connection to the server.
   Mostly useful in tests."
  [vc]
  (u/shutdown! vc))
