(ns com.dept24c.vivo.react
  (:require
   #?(:cljs ["react" :as React])
   #?(:cljs ["react-dom/server" :as ReactDOMServer])
   [clojure.core.async :as ca]
   [com.dept24c.vivo.macro-impl :as macro-impl]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.logging :as log]
   #?(:cljs [goog.object])
   #?(:cljs [oops.core :refer [oapply ocall oget oset!]]))
  #?(:cljs
     (:require-macros com.dept24c.vivo.react)))

(defn create-element [& args]
  #?(:cljs
     (oapply React :createElement args)))

(defn valid-element? [el]
  #?(:cljs
     (ocall React :isValidElement el)))

(defn render-to-string [el]
  #?(:cljs
     (ocall ReactDOMServer :renderToString el)))

(defn render-to-static-markup [el]
  #?(:cljs
     (ocall ReactDOMServer :renderToStaticMarkup el)))

(defn use-effect
  ([effect]
   #?(:cljs
      (ocall React :useEffect effect)))
  ([effect dependencies]
   #?(:cljs
      (ocall React :useEffect effect dependencies))))

(defn use-layout-effect
  ([effect]
   #?(:cljs
      (ocall React :useLayoutEffect effect)))
  ([effect dependencies]
   #?(:cljs
      (ocall React :useLayoutEffect effect dependencies))))

(defn use-reducer
  [reducer initial-state]
  #?(:cljs
     (ocall React :useReducer reducer initial-state)))

(defn use-ref
  ([]
   (use-ref nil))
  ([initial-value]
   #?(:cljs
      (ocall React :useRef initial-value))))

(defn use-state [initial-state]
  #?(:cljs
     (ocall React :useState initial-state)))

(defn use-callback [f deps]
  #?(:cljs
     (ocall React :useCallback f deps)))

(defn with-key [element k]
  "Adds the given React key to element."
  #?(:cljs
     (ocall React :cloneElement element #js {"key" k})))

(defn <ssr
  ([vc component-fn component-name]
   (u/<ssr vc component-fn component-name false))
  ([vc component-fn component-name static-markup?]
   (u/<ssr vc component-fn component-name static-markup?)))

;;;; Macros

(defmacro def-component
  "Defines a Vivo React component.
   The first argument to the constructor must be a parameter named
  `vc` (the vivo client)."
  [component-name & args]
  (let [ns-name (str (or
                      (:name (:ns &env)) ;; cljs
                      *ns*))]            ;; clj
    (macro-impl/build-component ns-name component-name args)))

(defmacro def-component-method
  "Defines a Vivo React component multimethod"
  [component-name dispatch-val & args]
  (let [ns-name (str (or
                      (:name (:ns &env)) ;; cljs
                      *ns*))]            ;; clj
    (macro-impl/build-component ns-name component-name args dispatch-val)))


;;;; Custom Hooks

(defn use-vivo-state
  "React hook for Vivo"
  ([vc sub-map component-name]
   (use-vivo-state vc sub-map component-name {} []))
  ([vc sub-map component-name resolution-map]
   (use-vivo-state vc sub-map component-name resolution-map []))
  ([vc sub-map component-name resolution-map parents]
   #?(:cljs
      ;; During SSR, we need to render the correct state immediately, since
      ;; there will never be any updates later.
      (let [initial-state (if (u/ssr? vc)
                            (u/ssr-get-state! vc sub-map resolution-map)
                            :vivo/unknown)

            [state update-fn] (use-state initial-state)
            *mounted? (atom true)
            effect (fn []
                     (let [unsub (u/subscribe! vc sub-map state
                                               #(when @*mounted?
                                                  (update-fn %))
                                               component-name
                                               resolution-map
                                               parents)]
                       (fn on-unmount []
                         (reset! *mounted? false)
                         (unsub))))]
        (use-effect effect (clj->js [sub-map resolution-map]))
        state))))

;;;;;;;;;;;;;;;;;;;; Macro runtime helpers ;;;;;;;;;;;;;;;;;;;;
;; Emitted code calls these fns

(defn get* [js-obj k]
  #?(:cljs
     (goog.object/get js-obj k)))

(defn js-obj* [kvs]
  #?(:cljs
     (apply goog.object/create kvs)))

(def ^:dynamic *parents* [])
