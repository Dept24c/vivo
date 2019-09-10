(ns com.dept24c.vivo
  (:require
   [clojure.core.async :as ca]
   [com.dept24c.vivo.state :as state]
   [com.dept24c.vivo.utils :as u]))

(defn state-manager
  "Creates a Vivo state manager."
  ([]
   (state-manager {}))
  ([opts]
   (state/state-manager opts)))

(defn subscribe!
  "Creates a Vivo subscription. When any of the paths in the `sub-map`
   change, calls `update-fn` with the updated state. Note that this
   is a low-level function that generally should not be called directly.
   Prefer `def-component` or `use-vivo-state`.
   Returns a subscription id."
  [sm sub-map cur-state update-fn subscriber-name]
  (u/subscribe! sm sub-map cur-state update-fn subscriber-name))

(defn unsubscribe!
  "Removes a Vivo subscription. Returns nil."
  [sm sub-id]
  (u/unsubscribe! sm sub-id))

(defn update-state!
  ([sm update-commands]
   (update-state! sm update-commands nil))
  ([sm update-commands cb]
   (u/update-state! sm update-commands cb)))

(defn <update-state!
  ([sm update-commands]
   (let [ch (ca/chan)
         cb #(ca/put! ch %)]
     (u/update-state! sm update-commands cb)
     ch)))

(defn set-state!
  ([sm path arg]
   (set-state! sm path arg nil))
  ([sm path arg cb]
   (u/update-state! sm [{:path path
                         :op :set
                         :arg arg}] cb)))

(defn <set-state!
  ([sm path arg]
   (let [ch (ca/chan)
         cb #(ca/put! ch %)]
     (set-state! sm path arg cb)
     ch)))

;; TODO: Move this to react ns?
(defn log-in!
  ([sm identifier secret]
   (u/log-in! sm identifier secret nil))
  ([sm identifier secret cb]
   (u/log-in! sm identifier secret cb)))

(defn <log-in!
  [sm identifier secret]
  (let [ch (ca/chan)
        cb #(ca/put! ch %)]
    (u/log-in! sm identifier secret cb)
    ch))

(defn log-out!
  "Log out from the Vivo server."
  [sm]
  (u/log-out! sm))

(defn <add-subject!
  ([sm identifier secret]
   (<add-subject! sm identifier secret nil))
  ([sm identifier secret subject-id]
   (u/<add-subject! sm identifier secret subject-id)))

(defn shutdown!
  "Shutdown the state manager and its connection to the server.
   Mostly useful in tests."
  [sm]
  (u/shutdown! sm))
