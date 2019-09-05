(ns com.dept24c.vivo.react
  (:require
   ["react" :as React]
   ["react-dom/server" :as ReactDOMServer]
   [com.dept24c.vivo.utils :as u]
   [oops.core :refer [oapply ocall]]))

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

(defn use-ref [initial-value]
  (ocall React :useRef initial-value))

(defn use-state [initial-state]
  (ocall React :useState initial-state))

(defn with-key [element k]
  (ocall React :cloneElement element (clj->js {"key" k})))
