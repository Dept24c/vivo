(ns com.dept24c.vivo
  (:require
   [com.dept24c.vivo.bristlecone-state-provider-impl :as bspi]
   [com.dept24c.vivo.macro-impl :as macro-impl]
   [com.dept24c.vivo.mem-state-provider-impl :as mspi]
   [com.dept24c.vivo.state :as state]
   [com.dept24c.vivo.state-manager-impl :as state-manager-impl])
  #?(:cljs
     (:require-macros com.dept24c.vivo)))

(defmacro def-component
  "Defines a Vivo React component. You may optionally provide a
   subscription map. The first argument to the constructor must
   be a parameter named `sm` (a state manager)."
  [component-name & args]
  (macro-impl/build-component component-name args))

(defmacro def-subscriber
  "Defines a Vivo non-visual subscriber. A subscription map is required.
   The first argument to the constructor must be a parameter named
  `sm` (a state manager)."
  [subscriber-name & args]
  (macro-impl/build-subscriber subscriber-name args))
(defn state-manager
  "Creates a state manager with the given mapping of root keys to
   state providers."
  [root-key->state-provider]
  (state-manager-impl/state-manager root-key->state-provider))

(defn mem-state-provider
  "Creates an in-memory, non-durable state provider. Optionally takes
   an initial state as an argument."
  ([]
   (mem-state-provider nil))
  ([initial-state]
   (mspi/mem-state-provider initial-state)))

(defn bristlecone-state-provider
  "Creates a state provider for use with a bristlecone server."
  ([get-server-url state-schema]
   (bristlecone-state-provider get-server-url state-schema {}))
  ([get-server-url state-schema opts]
   (bspi/bristlecone-state-provider get-server-url state-schema opts)))

(defn update-state!
  "Updates the state using the given update map, which is a map of paths
   to update expressions. If order is important, the update map can be
   replaced with a sequence of [path update-expresqsion] pairs."
  ([sm update-map]
   (update-state! sm update-map nil))
  ([sm update-map tx-info]
   (state/update-state! sm update-map tx-info nil)))

(defn subscribe!
  "Adds a Vivo subscription to the state manager. Generally, prefer the
   def-subsciber macro, as it provides quoting of the subscription map
   and a simpler interface."
  [sm sub-id sub-map update-fn]
  (state/subscribe! sm sub-id sub-map update-fn))
