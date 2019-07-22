(ns com.dept24c.vivo
  (:require
   [clojure.core.async :as ca]
   [com.dept24c.vivo.macro-impl :as macro-impl]
   [com.dept24c.vivo.state :as state]
   #?(:cljs ["react" :as React])
   #?(:cljs ["react-dom" :as ReactDOM]))
  #?(:cljs
     (:require-macros com.dept24c.vivo)))

(defn state-manager
  ([]
   (state-manager {}))
  ([opts]
   (state/state-manager opts)))

(defmacro def-component
  "Defines a Vivo React component. You may optionally provide a
   subscription map. The first argument to the constructor must
   be a parameter named `sm` (a state manager)."
  [component-name & args]
  (macro-impl/build-component component-name args))

(defn subscribe!
  "Creates a Vivo subscription. When any of the paths in the `sub-map`
   change, calls `update-fn` with the updated state.
   Returns a subscription id."
  [sm sub-map cur-state update-fn]
  (state/subscribe! sm sub-map cur-state update-fn))

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
     (state/set-state! sm path arg cb)
     ch)))

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
  ([sm]
   (state/log-out! sm nil))
  ([sm cb]
   (state/log-out! sm cb)))

(defn <log-out!
  [sm]
  (let [ch (ca/chan)
        cb #(ca/put! ch %)]
    (state/log-out! sm cb)
    ch))

#?(:cljs
   (defn use-vivo-state
     "React hook for Vivo"
     [sm sub-map]
     (let [[state update-fn] (.useState React nil)
           effect (fn []
                    (let [sub-id (subscribe! sm sub-map state update-fn)]
                      #(unsubscribe! sm sub-id)))]
       (.useEffect React effect)
       state)))

(defn shutdown!
  "Shutdown the state manager and its connection to the server.
   Mostly useful in tests."
  [sm]
  (state/shutdown! sm))
