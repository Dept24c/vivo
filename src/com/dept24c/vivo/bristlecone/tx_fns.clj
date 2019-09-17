(ns com.dept24c.vivo.bristlecone.tx-fns
  (:require
   [com.dept24c.vivo.commands :as commands]
   [com.dept24c.vivo.utils :as u]
   [clojure.set :as set]
   [deercreeklabs.async-utils :as au]
   [weavejester.dependency :as dep]))

(defn get-in-state [state path prefix]
  (if-not (some sequential? path)
    (:val (commands/get-in-state state path prefix))
    (reduce-kv (fn [acc k expanded-path]
                 (assoc acc k (get-in-state state expanded-path prefix)))
               {} (u/expand-path path))))

(defn make-state-info [tx-fn-name ordered-pairs state]
  (reduce (fn [acc [sym path]]
            (let [resolved-path (u/resolve-symbols-in-path
                                 acc ordered-pairs tx-fn-name path)
                  v (get-in-state state resolved-path :sys)]
              (-> acc
                  (assoc-in [:state sym] v)
                  (update :paths conj (or resolved-path path)))))
          {:state {}
           :paths []}
          ordered-pairs))

(defn throw-bad-output-path [output-path]
  (throw (ex-info (str "Transaction fn :output-path must begin with "
                       "`:sys`. Got `" output-path "`.")
                  (u/sym-map output-path))))

(defn eval-tx-fns [sub-map->op-cache tx-fns state]
  (reduce (fn [acc tx-fn]
            (let [{:keys [name sub-map f output-path]} tx-fn
                  [head & tail] output-path
                  _ (when-not (= :sys head)
                      (throw-bad-output-path output-path))
                  pairs (u/sub-map->ordered-pairs sub-map->op-cache sub-map)
                  si (make-state-info name pairs state)]
              (-> acc
                  (update :state assoc-in tail (f (:state si)))
                  (update :updated-paths conj output-path))))
          {:state state
           :updated-paths []}
          tx-fns))
