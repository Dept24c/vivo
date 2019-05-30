(ns com.dept24c.vivo
  (:require
   [clojure.core.async :as ca]
   [com.dept24c.vivo.macro-impl :as macro-impl]
   [com.dept24c.vivo.state :as state])
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
  "Returns nil."
  [sm sub-id sub-map update-fn]
  (state/subscribe! sm sub-id sub-map update-fn))

(defn unsubscribe!
  "Returns nil."
  [sm sub-id]
  (state/unsubscribe! sm sub-id))

(defn update-state!
  ([sm update-commands]
   (update-state! sm update-commands nil))
  ([sm update-commands tx-info]
   (state/update-state! sm update-commands tx-info nil)))

(defn <update-state!
  ([sm update-commands]
   (<update-state! sm update-commands nil))
  ([sm update-commands tx-info]
   (let [ch (ca/chan)
         cb #(ca/put! ch %)]
     (state/update-state! sm update-commands tx-info cb)
     ch)))
