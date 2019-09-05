(ns com.dept24c.vivo.react
  (:require
   ["react" :as React]
   ["react-dom/server" :as ReactDOMServer]
   [com.dept24c.vivo.utils :as u]
   [oops.core :refer [ocall]]))

(defn create-element [& args]
  (apply (.-createElement React) args))

(defn use-state [initial-state]
  (.useState React initial-state))

(defn use-effect
  ([effect]
   (use-effect effect nil))
  ([effect dependencies]
   (if dependencies
     (.useEffect React effect dependencies)
     (.useEffect React effect))))

(defn render-to-string [el]
  (ocall ReactDOMServer :renderToString el))

(defn is-valid-element? [el]
  (ocall React :isValidElement el))

(defn with-key [element k]
  (ocall React :cloneElement element (clj->js {"key" k})))
