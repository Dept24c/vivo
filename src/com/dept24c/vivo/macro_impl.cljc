(ns com.dept24c.vivo.macro-impl
  (:require
   [clojure.core.async :as ca]
   [clojure.set :as set]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au])
  #?(:cljs
     (:require-macros
      [com.dept24c.vivo.macro-impl :refer [get-locals-map]])
     :clj
     (:import
      (clojure.lang ExceptionInfo))))

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
                   (not= "vc" (name first-arg))))
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

(defmacro get-locals-map []
  (let [ks (-> (:locals &env)
               (keys)
               (set)
               (disj 'vc)
               (vec))]
    `(zipmap (quote ~ks) ~ks)))

(defn make-sub-body [parts component-name]
  (let [{:keys [sub-map body]} parts
        sub-syms (keys sub-map)
        cname (name component-name)
        inner-component-name (symbol (str cname "-inner"))]
    `(let [vivo-state# (com.dept24c.vivo.react/use-vivo-state
                        ~'vc '~sub-map ~cname ~'*vivo-locals*)
           {:syms [~@sub-syms]} vivo-state#]
       (when vivo-state#
         (com.dept24c.vivo.react/create-element
          (fn ~inner-component-name [props#]
            ~@body))))))

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
         component-body (if sub-map
                          [(make-sub-body parts component-name)]
                          body)]
     `(~@first-line
       [~@arglist]
       (let [~'*vivo-locals* (get-locals-map)]
         (com.dept24c.vivo.react/create-element
          (fn ~component-name [props#]
            ~@component-body)))))))
