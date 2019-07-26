(ns com.dept24c.vivo.macro-impl
  (:require
   [clojure.core.async :as ca]
   [clojure.set :as set]
   [com.dept24c.vivo.state :as state]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au])
  #?(:clj
     (:import
      (clojure.lang ExceptionInfo))))

(defn check-arglist [component-name arglist]
  (when-not (vector? arglist)
    (throw
     (ex-info (str "Illegal argument list in component `" component-name
                   "`. The argument list must be a vector. Got: `" arglist
                   "` which is a " (type arglist) ".")
              (u/sym-map arglist component-name))))
  (let [first-arg (first arglist)]
    (when (or (nil? first-arg)
              (not= "sm" (name first-arg)))
      (throw
       (ex-info (str "Bad constructor arglist for component `" component-name
                     "`. First argument must be `sm`"
                     " (the state manager). Got: `" first-arg "`.")
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
  (let [parts (if (string? (first args))
                (let [[docstring sub-map arglist & body] args]
                  (u/sym-map docstring sub-map arglist body))
                (let [docstring nil
                      [sub-map arglist & body] args]
                  (u/sym-map docstring sub-map arglist body)))
        {:keys [docstring sub-map arglist body]} parts
        repeated-syms (vec (set/intersection (set (keys sub-map))
                                             (set arglist)))]
    (u/check-sub-map component-name "component" sub-map)
    (check-arglist component-name arglist)
    (when (seq repeated-syms)
      (throw
       (ex-info (str "Illegal repeated symbol(s) in component "
                     component-name "`. The same symbol may not appear in "
                     "both the subscription map and the argument list. "
                     "Repeated symbols: " repeated-syms)
                (u/sym-map repeated-syms sub-map arglist component-name))))
    parts))

(defn build-component [component-name args]
  (let [parts (parse-def-component-args component-name args)
        {:keys [docstring sub-map arglist body]} parts
        sub-syms (keys sub-map)
        cname (name component-name)]
    `(defn ~component-name
       {:doc ~docstring}
       [~@arglist]
       (check-constructor-args ~cname '~arglist ~(count arglist))
       (let [vivo-state# (state/use-vivo-state ~'sm '~sub-map)
             {:syms [~@sub-syms]} vivo-state#]
         (when vivo-state#
           ~@body)))))
