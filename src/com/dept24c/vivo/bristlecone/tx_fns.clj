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
    (map #(get-in-state state % prefix) (u/expand-path path))))

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
                  _ (when-not (= :sys (first output-path))
                      (throw-bad-output-path output-path))
                  pairs (u/sub-map->ordered-pairs sub-map->op-cache sub-map)
                  si (make-state-info name pairs state)
                  ;; TODO: Optimize this; just need path. Don't need val.
                  {:keys [norm-path]} (commands/get-in-state state output-path)
                  op :set
                  val (f (:state si))
                  update-info (u/sym-map norm-path op val)
                  [head & tail] norm-path]
              (-> acc
                  (update :state assoc-in tail val)
                  (update :update-infos conj update-info))))
          {:state state
           :update-infos []}
          tx-fns))
