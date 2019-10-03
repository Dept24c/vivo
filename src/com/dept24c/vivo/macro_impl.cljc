(ns com.dept24c.vivo.macro-impl
  (:require
   [clojure.core.async :as ca]
   [clojure.set :as set]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au])
  #?(:clj
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

(defn check-constructor-args [subscriber-name args num-args-defined]
  (let [num-args-passed (count args)]
    (when-not (= num-args-defined num-args-passed)
      (throw
       (ex-info (str "Wrong number of arguments passed to `" subscriber-name
                     "`. Should have gotten " num-args-defined " arg(s), got "
                     num-args-passed".")
                (u/sym-map subscriber-name args num-args-passed
                           num-args-defined))))))

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

(defn make-sub-body [parts component-name]
  (let [{:keys [sub-map arglist body]} parts
        sub-syms (keys sub-map)
        cname (name component-name)]
    `(let [resolution-map# (zipmap (next '~arglist)
                                   (next (vector ~@arglist)))
           ~'*vivo-state* (com.dept24c.vivo.react/use-vivo-state
                           ~'vc '~sub-map ~cname resolution-map#)
           {:syms [~@sub-syms]} ~'*vivo-state*]
       ~@body)))

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
       (check-constructor-args ~component-name '~arglist ~(count arglist))
       (com.dept24c.vivo.react/create-element
        (fn ~component-name [props#]
          ~@component-body))))))
