(ns com.dept24c.vivo.macro-impl
  (:require
   [clojure.core.async :as ca]
   [clojure.walk :as walk]
   [clojure.set :as set]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [deercreeklabs.capsule.logging :as log]))

(defn check-arglist [component-name subscription? arglist]
  (when-not (vector? arglist)
    (throw
     (ex-info (str "Illegal argument list in component `" component-name
                   "`. The argument list must be a vector. Got: `" arglist
                   "` which is a " (type arglist) ".")
              (u/sym-map arglist component-name))))
  (let [first-arg (first arglist)]
    (when (and subscription?
               (or (nil? first-arg)
                   (not= "vc" (when (symbol? first-arg)
                                (name first-arg)))))
      (throw
       (ex-info (str "Bad constructor arglist for component `" component-name
                     "`. First argument must be `vc`"
                     " (the vivo client). Got: `" first-arg "`.")
                (u/sym-map component-name first-arg arglist))))))

;; TODO: Improve error msgs for incorrect args (wrong num args, etc.)
(defn parse-def-component-args [component-name args]
  (let [parts (if (string? (nth args 0))
                (if (map? (nth args 2))
                  (let [[docstring arglist sub-map & body] args]
                    (u/sym-map docstring arglist sub-map body))
                  (let [[docstring arglist & body] args]
                    (u/sym-map docstring arglist body)))
                (if (map? (nth args 1))
                  (let [[arglist sub-map & body] args]
                    (u/sym-map arglist sub-map body))
                  (let [[arglist & body] args]
                    (u/sym-map arglist body))))
        {:keys [docstring sub-map arglist body]} parts
        _ (check-arglist component-name (boolean sub-map) arglist)
        _ (when sub-map
            (u/check-sub-map component-name "component" sub-map))
        repeated-syms (vec (set/intersection (set (keys sub-map))
                                             (set arglist)))]
    (when (seq repeated-syms)
      (throw
       (ex-info (str "Illegal repeated symbol(s) in component "
                     component-name "`. The same symbol may not appear in "
                     "both the subscription map and the argument list. "
                     "Repeated symbols: " repeated-syms)
                (u/sym-map repeated-syms sub-map arglist component-name))))
    parts))

(defn destructure*
  ;; Work around fact that clojure.core/destructure returns code that
  ;; is not cljs-compatible
  [bindings]
  #?(:clj
     (walk/postwalk-replace
      {'clojure.lang.PersistentHashMap/create 'hash-map}
      (destructure bindings))))

(defn make-outer-body [sub-map props-sym cname-str inner-component-name]
  `(let [body-fn# (com.dept24c.vivo.react/get* ~props-sym "body-fn")
         vc# (com.dept24c.vivo.react/get* ~props-sym "vc")
         sub-map# (com.dept24c.vivo.react/get* ~props-sym "sub-map")
         resolution-map# (com.dept24c.vivo.react/get* ~props-sym
                                                      "resolution-map")
         cname-str# (com.dept24c.vivo.react/get* ~props-sym "cname-str")
         vivo-state# (com.dept24c.vivo.react/use-vivo-state
                      vc# sub-map# cname-str# resolution-map#)
         inner-props# (com.dept24c.vivo.react/js-obj*
                       ["body-fn" body-fn#
                        "vivo-state" vivo-state#])]
     (when (and vivo-state# (not= :vivo/unknown vivo-state#))
       (com.dept24c.vivo.react/create-element ~inner-component-name
                                              inner-props#))))

(defn build-component
  ([component-name args]
   (build-component component-name args nil))
  ([component-name args dispatch-val]
   (let [parts (parse-def-component-args component-name args)
         {:keys [docstring arglist sub-map body]} parts
         first-line (if dispatch-val
                      `(defmethod ~component-name ~dispatch-val)
                      (cond-> (vec `(defn ~component-name))
                        docstring (conj docstring)))
         cname (name component-name)
         body-fn-name (symbol (str cname "-body-fn"))
         inner-component-name (gensym (str cname "-inner"))
         outer-component-name (gensym (str cname "-outer"))
         props-sym (gensym "props")
         arglist-raw-sym (gensym "arglist-raw")
         destructured (destructure* [arglist arglist-raw-sym])
         inner-component-form `(defn ~inner-component-name [~props-sym]
                                 (let [body-fn# (com.dept24c.vivo.react/get*
                                                 ~props-sym "body-fn")
                                       vivo-state# (com.dept24c.vivo.react/get*
                                                    ~props-sym "vivo-state")]
                                   (body-fn# vivo-state#)))
         outer-component-form `(defn ~outer-component-name [~props-sym]
                                 ~(make-outer-body sub-map props-sym cname
                                                   inner-component-name))
         res-map-ks (-> (take-nth 2 destructured)
                        (set)
                        (disj 'vc)
                        (vec))
         sub-map-ks (keys sub-map)
         vc-sym (if sub-map
                  `~'vc
                  `nil)
         c-form `(~@first-line
                  [& ~arglist-raw-sym]
                  (let [~@destructured
                        resolution-map# (zipmap (quote ~res-map-ks)
                                                ~res-map-ks)
                        body-fn# (fn ~body-fn-name [vivo-state#]
                                   (let [{:syms [~@sub-map-ks]} vivo-state#]
                                     ~@body))
                        prop-kvs# (reduce
                                   (fn [acc# k#]
                                     (-> acc#
                                         (conj (str k#))
                                         (conj (get resolution-map# k#))))
                                   ["body-fn" body-fn#
                                    "vc" ~vc-sym
                                    "sub-map" '~sub-map
                                    "resolution-map" resolution-map#
                                    "cname-str" ~cname]
                                   ~res-map-ks)
                        props# (com.dept24c.vivo.react/js-obj* prop-kvs#)]
                    (com.dept24c.vivo.react/create-element
                     ~(if sub-map
                        outer-component-name
                        inner-component-name)
                     props#)))
         forms (if sub-map
                 [inner-component-form outer-component-form c-form]
                 [inner-component-form c-form])]
     `(do
        ~@forms))))
