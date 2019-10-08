(ns com.dept24c.vivo.react
  (:require
   #?(:cljs ["react" :as React])
   #?(:cljs ["react-dom/server" :as ReactDOMServer])
   [clojure.core.async :as ca]
   [com.dept24c.vivo.macro-impl :as macro-impl]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   #?(:cljs [oops.core :refer [oapply ocall oget]]))
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
   (use-effect effect nil))
  ([effect dependencies]
   #?(:cljs
      (if dependencies
        (ocall React :useEffect effect dependencies)
        (ocall React :useEffect effect)))))

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

(defn local-or-vivo-only? [sub-map]
  (reduce (fn [acc path]
            (cond
              (#{:vivo/component-id :vivo/subscriber-id} path)
              (reduced false)

              (= :vivo/subject-id path)
              acc

              (= :local (first path))
              acc

              :else

              (reduced false)))
          true (vals sub-map)))

;;;; Macros

(defmacro def-component
  "Defines a Vivo React component.
   The first argument to the constructor must be a parameter named
  `vc` (the vivo client)."
  [component-name & args]
  (macro-impl/build-component component-name args))

(defmacro def-component-method
  "Defines a Vivo React component multimethod"
  [component-name dispatch-val & args]
  (macro-impl/build-component component-name args dispatch-val))

;;;; Custom Hooks

(defn use-vivo-state
  "React hook for Vivo"
  ([vc sub-map component-name]
   (use-vivo-state vc sub-map component-name {}))
  ([vc sub-map component-name resolution-map]
   #?(:cljs
      (let [initial-state (cond
                            (local-or-vivo-only? sub-map)
                            (u/get-local-state vc sub-map resolution-map
                                               component-name)

                            (u/ssr? vc)
                            (u/ssr-get-state! vc sub-map resolution-map)

                            :else
                            (u/get-cached-state vc sub-map resolution-map))
            [state update-fn] (use-state initial-state)
            effect (fn []
                     (let [unsub (u/subscribe! vc sub-map state
                                               #(update-fn %)
                                               component-name resolution-map)]
                       unsub))]
        (use-effect effect #js [])
        state))))
