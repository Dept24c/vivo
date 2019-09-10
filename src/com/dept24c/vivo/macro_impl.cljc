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

(defn check-initial-cstate [initial-cstate]
  (doseq [[k v] initial-cstate]
    (when-not (symbol? k)
      (throw (ex-info (str "Bad key `" k "` in initial component state. "
                           "Keys must be symbols.")
                      {:bad-key k
                       :initial-component-state initial-cstate})))))

(defn check-repeated-syms
  [component-name sub-map initial-component-state arglist]
  (let [pairs [[(set (keys sub-map)) "subscription map"]
               [(set (keys initial-component-state)) "initial component state"]
               [(set arglist) "argument list"]]]
    (doseq [[a b] [[0 1] [0 2] [1 2]]]
      (let [[a-syms a-description] (nth pairs a)
            [b-syms b-description] (nth pairs b)
            repeated-syms (seq (set/intersection a-syms b-syms))]
        (when repeated-syms
          (throw (ex-info
                  (str "Illegal repeated symbol(s) in component `"
                       component-name "`. The same symbol may not appear in "
                       "both the " a-description " and the " b-description ". "
                       "Repeated symbols: " repeated-syms)
                  (u/sym-map repeated-syms sub-map initial-component-state
                             arglist component-name))))))))

(defn parse-def-component-args [component-name args]
  (let [docstring (when (string? (first args))
                    (first args))
        sub-map (nth args (if docstring 1 0))
        initial-cstate (if docstring
                         (when (map? (nth args 2))
                           (nth args 2))
                         (when (map? (nth args 1))
                           (nth args 1)))
        arglist-i (case [(boolean docstring) (boolean initial-cstate)]
                    [true true] 3
                    [true false] 2
                    [false true] 2
                    [false false] 1)
        arglist (nth args arglist-i)
        body (seq (drop (inc arglist-i) args))
        parts (u/sym-map docstring sub-map initial-cstate arglist body)
        repeated-syms (vec (set/intersection (set (keys sub-map))
                                             (set (keys initial-cstate))
                                             (set arglist)))]
    (u/check-sub-map component-name "component" sub-map)
    (when initial-cstate
      (check-initial-cstate initial-cstate))
    (check-arglist component-name arglist)
    (check-repeated-syms component-name sub-map initial-cstate arglist)
    parts))

(defn build-component [component-name args]
  (let [parts (parse-def-component-args component-name args)
        {:keys [docstring sub-map initial-cstate arglist body]} parts
        sub-syms (keys sub-map)
        cstate-syms (keys initial-cstate)
        cname (name component-name)]
    `(defn ~component-name
       {:doc ~docstring}
       [~@arglist]
       (check-constructor-args ~cname '~arglist ~(count arglist))
       (com.dept24c.vivo.react/create-element
        (fn ~component-name [props#]
          (let [vivo-state# (com.dept24c.vivo.react/use-vivo-state
                             ~'sm '~sub-map ~cname)
                [cstate# set-cstate#] (com.dept24c.vivo.react/use-state
                                       '~initial-cstate)
                {:syms [~@sub-syms]} vivo-state#
                {:syms [~@cstate-syms]} cstate#
                ~'set-component-state! (fn [sym# v#]
                                         (when-not (symbol? sym#)
                                           (throw
                                            (ex-info
                                             (str
                                              "First arg to "
                                              "set-component-state! must be a "
                                              "symbol. Got `" sym# "`.")
                                             {:args [sym# v#]})))
                                         (-> (assoc cstate# sym# v#)
                                             (set-cstate#)))]
            (when vivo-state#
              ~@body)))))))
