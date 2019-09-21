(ns com.dept24c.vivo
  (:require
   [clojure.core.async :as ca]
   [com.dept24c.vivo.client :as client]
   #?(:clj [com.dept24c.vivo.server :as server])
   [com.dept24c.vivo.utils :as u]))

#?(:clj
   (defn vivo-server
     "Returns a no-arg fn that stops the server."
     [config]
     (server/vivo-server config)))

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
   Prefer `def-component` or `use-vivo-state`.
   Returns a subscription id."
  ([vc sub-map initial-state update-fn subscriber-name]
   (u/subscribe! vc sub-map initial-state update-fn subscriber-name {}))
  ([vc sub-map initial-state update-fn subscriber-name resolution-map]
   (u/subscribe! vc sub-map initial-state update-fn subscriber-name
                 resolution-map)))

(defn unsubscribe!
  "Removes a Vivo subscription. Returns nil."
  [vc sub-id]
  (u/unsubscribe! vc sub-id))

(defn update-state!
  ([vc update-commands]
   (update-state! vc update-commands nil))
  ([vc update-commands cb]
   (u/update-state! vc update-commands cb)))

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

(defn log-in!
  ([vc identifier secret]
   (u/log-in! vc identifier secret nil))
  ([vc identifier secret cb]
   (u/log-in! vc identifier secret cb)))

(defn <log-in!
  [vc identifier secret]
  (let [ch (ca/chan)
        cb #(ca/put! ch %)]
    (u/log-in! vc identifier secret cb)
    ch))

(defn log-out!
  "Log out from the Vivo server."
  [vc]
  (u/log-out! vc))

(defn <add-subject!
  ([vc identifier secret]
   (<add-subject! vc identifier secret nil))
  ([vc identifier secret subject-id]
   (u/<add-subject! vc identifier secret subject-id)))

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

(defn shutdown!
  "Shutdown the vivo client and its connection to the server.
   Mostly useful in tests."
  [vc]
  (u/shutdown! vc))
