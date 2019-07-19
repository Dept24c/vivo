(ns com.dept24c.vivo.macro-impl
  (:require
   [clojure.core.async :as ca]
   [clojure.set :as set]
   [com.dept24c.vivo.state-manager :as state-manager]
   [com.dept24c.vivo.utils :as u]
   [deercreeklabs.async-utils :as au]
   [rum.core :as rum])
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
  (let [[first-arg second-arg & others] args]

    (cond
      (sequential? first-arg)
      (do
        (check-arglist component-name first-arg)
        [nil first-arg (next args)])

      (map? first-arg)
      (do
        (let [[sub-map arglist] [first-arg second-arg]
              repeated-syms (vec (set/intersection (set (keys sub-map))
                                                   (set arglist)))]
          (check-arglist component-name arglist)
          (u/check-sub-map component-name "component" sub-map)
          (when (seq repeated-syms)
            (throw
             (ex-info
              (str "Illegal repeated symbol(s) in component "
                   component-name "`. The same symbol may not appear in "
                   "both the subscription map and the argument list. Repeated "
                   "symbols: " repeated-syms)
              (u/sym-map repeated-syms sub-map)))))
        [first-arg second-arg others])

      :else
      (throw
       (ex-info
        (str "Bad argument to component `" component-name
             "`. After the component name, you must provide either "
             "a subscription map or an argument list. Got: `"
             first-arg "`. type: " (type first-arg))
        (u/sym-map first-arg args))))))
#_
(defn subscribed-mixin
  [component-name num-args-defined sub-map]
  (let [*data-frame (atom nil)]
    {:init (fn [cstate props]
             (let [{:rum/keys [args]} cstate
                   state-manager (first args)
                   sub-id (str component-name "-" (rand-int 1e9))]
               (check-constructor-args component-name args num-args-defined)
               (merge cstate (u/sym-map component-name state-manager sub-id
                                        *data-frame))))
     ;; The state manager only requests an update when something has changed
     ;; so we can just return true here
     :should-update (constantly true)
     :will-mount (fn [cstate]
                   (let [{:keys [state-manager sub-id]} cstate
                         updated-ch (ca/promise-chan)
                         update-fn (fn [data-frame]
                                     (reset! *data-frame data-frame)
                                     #?(:clj (ca/put! updated-ch true))
                                     #?(:cljs
                                        (rum/request-render
                                         (:rum/react-component cstate))))]
                     (state-manager/subscribe! state-manager sub-id sub-map
                                               update-fn)
                     ;; For SSR, wait for update-fn to be called.
                     #?(:clj (au/<?? updated-ch))
                     cstate))
     :will-unmount (fn [cstate]
                     (let [{:keys [state-manager sub-id]} cstate]
                       (state-manager/unsubscribe! state-manager sub-id)
                       cstate))}))

(defn build-component [component-name args]
  #_
  (let [[sub-map arglist body] (parse-def-component-args component-name args)
        sub-syms (keys sub-map)
        cname (name component-name)
        num-args-defined (count arglist)]
    (if (seq sub-map)
      `(rum/defcs ~component-name
         ~'< (subscribed-mixin ~cname ~num-args-defined '~sub-map)
         [& cargs#]
         (let [[cstate# ~@arglist] cargs#
               {:syms [~@sub-syms] :as df#} (deref (:*data-frame cstate#))]
           (when df# ;; Don't render anything until df is filled in
             ~@body)))
      `(rum/defc ~component-name ~'< rum/static
         [& cargs#]
         (check-constructor-args ~cname cargs# ~num-args-defined)
         (let [[~@arglist] cargs#]
           ~@body)))))
