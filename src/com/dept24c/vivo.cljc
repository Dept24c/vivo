(ns com.dept24c.vivo
  (:require
   [clojure.core.async :as ca]
   [com.dept24c.vivo.client :as client]
   #?(:clj [com.dept24c.vivo.server :as server])
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.logging :as log]))

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

(defn subscribe-to-state-changes!
  "Creates a Vivo state subscription. When the state referred to by any
   of the paths in the `sub-map` changes, `update-fn` is called with the
    updated state. Note that this is a low-level function that generally
   should not be called directly. Prefer `react/def-component`
   or `react/use-vivo-state`.

   `opts` is a map of optional parameters:
     - `parents`: sequence of sub-names
     - `react?`: boolean. Enables react update batching
     - `resolution-map`: map of symbols to values to be used in resolving
                         symbols in values of the sub-map"
  ([vc sub-name sub-map update-fn]
   (subscribe-to-state-changes! vc sub-name sub-map update-fn {}))
  ([vc sub-name sub-map update-fn opts]
   (u/subscribe-to-state-changes! vc sub-name sub-map update-fn opts)))

(defn unsubscribe-from-state-changes!
  "Cancels a state-changes subscription and releases the related
   resources."
  [vc sub-name]
  (u/unsubscribe-from-state-changes! vc sub-name))

(defn subscribe-to-event!
  "Creates a Vivo event subscription.
   Parameters:
    - `vc`: the Vivo client
    - `scope`: Either `:local` or `:sys`.
    - `event-name`: The name of the event to subscribe to (string).
    - `cb`: A callback fn of one argument to be called when the event
            is received. The event's string value will be passed to the fn.
   Returns a zero-arg `unsubscribe!` fn that can be called to cancel
   the subscription and clean up related resources."
  [vc scope event-name cb]
  (u/subscribe-to-event! vc scope event-name cb))

(defn publish-event!
  "Publish an event to active subscribers.

   Parameters:
    - `vc`: the Vivo client
    - `scope`: Either `:local` or `:sys`.
    - `event-name`: The name of the event to publish (string).
    - `event-str`: The value of the event to publish (string)."
  [vc scope event-name event-str]
  (u/publish-event! vc scope event-name event-str))

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
  "If successful, returns a map with :subject-id and :token keys,
   otherwise returns false."
  [vc identifier secret]
  (u/<log-in! vc identifier secret))

(defn <log-in-w-token!
  "If successful, returns a map with :subject-id and :token keys,
   otherwise returns false."
  [vc token]
  (u/<log-in-w-token! vc token))

(defn <log-out!
  "Log out from the Vivo server."
  [vc]
  (u/<log-out! vc))

(defn <log-out-w-token!
  "Log out a user from the Vivo server using their token.
   Returns a boolean success value."
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

(def default-rpc-timeout-ms 30000)

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
         (log/error (str "Exception in rpc:\n" (u/ex-msg-and-stacktrace e)))
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

(defn <wait-for-conn-init
  "Returns a channel that yields either:
    - `true`: When the client connection is initialized.
    - `false`: When the client has been shut down.
    - an exception when more than 60 seconds elapse.
  Primarily useful for setting up testing environments."
  [vc]
  (u/<wait-for-conn-init vc))

(defn shutdown!
  "Shutdown the vivo client and its connection to the server.
   Mostly useful in tests."
  [vc]
  (u/shutdown! vc))
