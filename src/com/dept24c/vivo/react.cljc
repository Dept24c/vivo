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

(defn is-valid-element? [el]
  #?(:cljs
     (ocall React :isValidElement el)))

(defn render-to-string [el]
  #?(:cljs
     (ocall ReactDOMServer :renderToString el)))

(defn use-effect
  ([effect]
   (use-effect effect nil))
  ([effect dependencies]
   #?(:cljs
      (if dependencies
        (ocall React :useEffect effect dependencies)
        (ocall React :useEffect effect)))))

(defn use-ref
  ([]
   (use-ref nil))
  ([initial-value]
   #?(:cljs
      (ocall React :useRef initial-value))))

(defn use-state [initial-state]
  #?(:cljs
     (ocall React :useState initial-state)))

(defn with-key [element k]
  "Adds the given React key to element."
  #?(:cljs
     (ocall React :cloneElement element #js {"key" k})))

;;;; Macros

(defmacro component
  [& body]
  `(create-element
    (fn [props#]
      ~@body)))

(defmacro def-component
  "Defines a Vivo React component.
  The first argument to the constructor must
   be a parameter named `sm` (a state manager)."
  [component-name & args]
  (macro-impl/build-component component-name args))

;;;; SSR Support

(def initial-ssr-info {:resolved {}
                       :needed #{}})

(defn ssr? [sm]
  (boolean @(:*ssr-info sm)))

(defn ssr-get-state! [sm sub-map]
  (let [{:keys [*ssr-info]} sm]
    (or (get (:resolved @*ssr-info) sub-map)
        (do
          (swap! *ssr-info update :needed conj sub-map)
          nil))))

(defn <ssr
  "Perform a server-side rendering. Returns a string."
  [sm component-fn component-name]
  (when-not (ifn? component-fn)
    (throw (ex-info (str "component-fn must be a function. Got: `"
                         (or component-fn "nil") "`.")
                    (u/sym-map component-fn))))
  (ca/go
    (try
      (let [{:keys [*ssr-info]} sm]
        (when-not (compare-and-set! *ssr-info nil initial-ssr-info)
          (throw
           (ex-info (str "Another SSR is in progress. Try again...") {})))
        (try
          (loop []
            (let [el (component-fn sm)
                  _ (when-not (is-valid-element? el)
                      (throw (ex-info
                              (str "component-fn must return a valid React "
                                   "element. Returned: `" (or el "nil") "`.")
                              {:returned el})))
                  ;; This has side effects (populates *ssr-info)
                  s (render-to-string el)
                  {:keys [needed]} @*ssr-info]
              (if-not (seq needed)
                s
                (do
                  (doseq [sub-map needed]
                    (let [{:keys [state]} (au/<? (u/<make-state-info
                                                  sm sub-map
                                                  component-name))]
                      (swap! *ssr-info update
                             :resolved assoc sub-map state)))
                  (swap! *ssr-info assoc :needed #{})
                  (recur)))))
          (finally
            (reset! *ssr-info nil))))
      (catch #?(:clj Exception :cljs js/Error) e
        (println (str "Exception in <ssr:\n" (u/ex-msg-and-stacktrace e)))))))

;;;; Custom Hooks

(defn use-vivo-state
  "React hook for Vivo"
  [sm sub-map subscriber-name]
  #?(:cljs
     (let [initial-state (cond
                           (u/local-or-vivo-only? sub-map)
                           (u/get-local-state sm sub-map subscriber-name)

                           (ssr? sm)
                           (ssr-get-state! sm sub-map)

                           :else
                           nil)
           [state update-fn] (use-state initial-state)
           effect (fn []
                    (let [sub-id (u/subscribe! sm sub-map state update-fn
                                               subscriber-name)]
                      #(u/unsubscribe! sm sub-id)))]
       (use-effect effect #js [])
       state)))


(defn use-on-outside-click
  "Calls the given callback when a click happens outside the referenced element.
   Returns a reference which should be added as a `ref` property to the
   referenced element, e.g. `{:ref (react/use-on-outside-click close-menu)}"
  [cb]
  #?(:cljs
     (let [el-ref (use-ref)
           handle-click (fn [e]
                          (when-not (ocall el-ref "current.contains"
                                           (oget e :target))
                            (cb)))
           events ["mousedown" "touchstart"]
           effect (fn []
                    (doseq [e events]
                      (ocall js/document :addEventListener
                             e handle-click))
                    #(doseq [e events]
                       (ocall js/document :removeEventListener
                              e handle-click)))]
       (use-effect effect #js [])
       el-ref)))
