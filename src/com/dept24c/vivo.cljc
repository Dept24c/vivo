(ns com.dept24c.vivo
  (:require
   [clojure.core.async :as ca]
   [com.dept24c.vivo.macro-impl :as macro-impl]
   #?(:cljs [com.dept24c.vivo.react :as react])
   #?(:clj [com.dept24c.vivo.server :as server])
   [com.dept24c.vivo.state :as state])
  #?(:cljs
     (:require-macros com.dept24c.vivo)))

(defn state-manager
  "Creates a Vivo state manager."
  ([]
   (state-manager {}))
  ([opts]
   (state/state-manager opts)))

(defmacro def-component
  "Defines a Vivo React component.
  The first argument to the constructor must
   be a parameter named `sm` (a state manager)."
  [component-name & args]
  (macro-impl/build-component component-name args))

(defn use-vivo-state
  "React hook for Vivo"
  ([sm sub-map]
   (use-vivo-state sm sub-map nil))
  ([sm sub-map component-name]
   (state/use-vivo-state sm sub-map component-name)))

(defn subscribe!
  "Creates a Vivo subscription. When any of the paths in the `sub-map`
   change, calls `update-fn` with the updated state. Note that this
   is a low-level function that generally should not be called directly.
   Prefer `def-component` or `use-vivo-state`.
   Returns a subscription id."
  [sm sub-map cur-state update-fn subscriber-name]
  (state/subscribe! sm sub-map cur-state update-fn subscriber-name))

(defn unsubscribe!
  "Removes a Vivo subscription. Returns nil."
  [sm sub-id]
  (state/unsubscribe! sm sub-id))

(defn update-state!
  ([sm update-commands]
   (update-state! sm update-commands nil))
  ([sm update-commands cb]
   (state/update-state! sm update-commands cb)))

(defn <update-state!
  ([sm update-commands]
   (let [ch (ca/chan)
         cb #(ca/put! ch %)]
     (state/update-state! sm update-commands cb)
     ch)))

(defn set-state!
  ([sm path arg]
   (set-state! sm path arg nil))
  ([sm path arg cb]
   (state/update-state! sm [{:path path
                             :op :set
                             :arg arg}] cb)))

(defn <set-state!
  ([sm path arg]
   (let [ch (ca/chan)
         cb #(ca/put! ch %)]
     (set-state! sm path arg cb)
     ch)))

(defn <ssr
  "Perform a server-side rendering. Returns a string."
  ([sm component-fn]
   (<ssr sm component-fn nil))
  ([sm component-fn component-name]
   (state/<ssr sm component-fn component-name)))

(defn log-in!
  ([sm identifier secret]
   (state/log-in! sm identifier secret nil))
  ([sm identifier secret cb]
   (state/log-in! sm identifier secret cb)))

(defn <log-in!
  [sm identifier secret]
  (let [ch (ca/chan)
        cb #(ca/put! ch %)]
    (state/log-in! sm identifier secret cb)
    ch))

(defn log-out!
  "Log out from the Vivo server."
  [sm]
  (state/log-out! sm))

(defn <add-subject!
  ([sm identifier secret]
   (<add-subject! sm identifier secret nil))
  ([sm identifier secret subject-id]
   (state/<add-subject! sm identifier secret subject-id)))

(defn shutdown!
  "Shutdown the state manager and its connection to the server.
   Mostly useful in tests."
  [sm]
  (state/shutdown! sm))

#?(:clj
   (defn vivo-server
     "Returns a no-arg fn that stops the server."
     [config]
     (server/vivo-server config)))

#?(:cljs
   (defn with-key
     "Adds the given React key to element."
     [element k]
     (react/with-key element k)))
