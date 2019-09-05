(ns com.dept24c.vivo.react
  (:require
   ["react" :as React]
   ["react-dom/server" :as ReactDOMServer]
   [com.dept24c.vivo.utils :as u]
   [oops.core :refer [oapply ocall oget]]))

(defn create-element [& args]
  (oapply React :createElement args))

(defn is-valid-element? [el]
  (ocall React :isValidElement el))

(defn render-to-string [el]
  (ocall ReactDOMServer :renderToString el))

(defn use-effect
  ([effect]
   (use-effect effect nil))
  ([effect dependencies]
   (if dependencies
     (ocall React :useEffect effect dependencies)
     (ocall React :useEffect effect))))

(defn use-ref
  ([]
   (use-ref nil))
  ([initial-value]
   (ocall React :useRef initial-value)))

(defn use-state [initial-state]
  (ocall React :useState initial-state))

(defn with-key [element k]
  (ocall React :cloneElement element #js {"key" k}))

;; Custom Hooks

(defn use-on-outside-click
  "Calls the given callback when a click outside the referenced element.
   Returns a reference which should be added as a `ref` property to the
   referenced element, e.g. `{:ref (react/use-on-outside-click close-menu)}"
  [cb]
  (let [el-ref (use-ref)
        handle-outside-click (fn [e]
                               (when-not (ocall el-ref "current.contains"
                                                (oget e :target))
                                 (cb)))
        effect (fn []
                 (ocall js/document :addEventListener "mousedown"
                        handle-outside-click)
                 #(ocall js/document :removeEventListener "mousedown"
                         handle-outside-click))]
    (use-effect effect #js [])
    el-ref))
