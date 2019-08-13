(ns com.dept24c.vivo.bristlecone.tx-fns
  (:require
   [com.dept24c.vivo.commands :as commands]
   [com.dept24c.vivo.utils :as u]
   [clojure.set :as set]
   [deercreeklabs.async-utils :as au]
   [weavejester.dependency :as dep]))

(defn throw-bad-path-key [path k]
  (let [disp-k (or k "nil")]
    (throw (ex-info
            (str "Illegal key `" disp-k "` in path `" path "`. Only integers, "
                 "keywords, symbols, and strings are valid path keys.")
            (u/sym-map k path)))))

(defn get-undefined-syms [sub-map]
  (let [defined-syms (set (keys sub-map))]
    (vec (reduce-kv (fn [acc sym v]
                      (if (sequential? v)
                        (reduce (fn [acc* k]
                                  (if-not (symbol? k)
                                    acc*
                                    (if (defined-syms k)
                                      acc*
                                      (conj acc* k))))
                                acc v)
                        (throw (ex-info "Bad path. Must be a sequence."
                                        (u/sym-map sym v sub-map)))))
                    #{} sub-map))))

(defn check-path [path sub-syms sub-map]
  (reduce (fn [acc k]
            (when (and (symbol? k) (not (sub-syms k)))
              (throw (ex-info
                      (str "Path symbol `" k "` in path `" path
                           "` is not defined as a key in the subscription map.")
                      (u/sym-map path k sub-map))))
            (if-not (or (keyword? k) (int? k) (string? k) (symbol? k))
              (throw-bad-path-key path k)
              (conj acc k)))
          [] path))

(defn make-sub-info [sub-map]
  (let [sub-syms (set (keys sub-map))
        info (reduce-kv
              (fn [acc sym v]
                (when-not (symbol? sym)
                  (throw (ex-info
                          (str "All keys in sub-map must be symbols. Got `"
                               sym "`.")
                          (u/sym-map sym sub-map))))
                (let [path v
                      [head & tail] (check-path path sub-syms sub-map)
                      deps (filter symbol? path)]
                  (cond-> (update acc :sym->path assoc sym path)
                    (seq deps) (update :g #(reduce (fn [g dep]
                                                     (dep/depend g sym dep))
                                                   % deps)))))
              {:tx-info-sym nil
               :g (dep/graph)
               :sym->path {}}
              sub-map)
        {:keys [tx-info-sym g sym->path]} info
        ordered-dep-syms (dep/topo-sort g)
        no-dep-syms (set/difference (set (keys sym->path))
                                    (set ordered-dep-syms))
        ordered-sym-path-pairs (reduce (fn [acc sym]
                                         (let [path (sym->path sym)]
                                           (conj acc [sym path])))
                                       []
                                       (concat (seq no-dep-syms)
                                               ordered-dep-syms))]
    (u/sym-map ordered-sym-path-pairs tx-info-sym)))

(defn xf-tx-fn [tf]
  (let [{:keys [sub-map f output-path]} tf]
    (when-not sub-map
      (throw (ex-info "Transaction function must have a :sub-map."
                      {:sub-map sub-map
                       :output-path output-path})))
    (when-not (map? sub-map)
      (throw (ex-info "Transaction function's :sub-map must be a map."
                      {:sub-map sub-map
                       :output-path output-path})))
    (let [undefined-syms (get-undefined-syms sub-map)]
      (when (seq undefined-syms)
        (throw
         (ex-info
          (str "Undefined symbol(s) in subscription map for transaction  "
               "function. These symbols are used in subscription "
               "keypaths, but are not defined: " undefined-syms)
          (u/sym-map undefined-syms sub-map output-path)))))
    (when-not (pos? (count sub-map))
      (throw (ex-info
              (str "Transaction function's :sub-map must contain at "
                   "least one entry.")
              {:sub-map sub-map
               :output-path output-path})))
    (when-not f
      (throw (ex-info "Transaction function must have an :f key and value."
                      {:sub-map sub-map
                       :output-path output-path})))
    (when-not output-path
      (throw (ex-info "Transaction function must have an :output-path."
                      {:sub-map sub-map
                       :output-path output-path})))
    (let [info (make-sub-info sub-map)
          {:keys [ordered-sym-path-pairs tx-info-sym]} info]
      (u/sym-map ordered-sym-path-pairs tx-info-sym f output-path))))

(defn make-tx-fn-data-frame [tf state]
  (let [{:keys [ordered-sym-path-pairs tx-info-sym]} tf
        last-i (dec (count ordered-sym-path-pairs))]
    (loop [df {}
           i 0]
      (let [[sym path] (nth ordered-sym-path-pairs i)
            resolved-path (reduce (fn [acc k]
                                    (if-not (symbol? k)
                                      (conj acc k)
                                      (if-let [v (df k)]
                                        (conj acc v)
                                        (reduced nil))))
                                  [] path)
            v (:val (commands/get-in-state state resolved-path))
            new-df (assoc df sym v)]
        (if (= last-i i)
          new-df
          (recur new-df (inc i)))))))

(defn eval-tx-fns [tx-fns state]
  (let [xfed-tx-fns (mapv xf-tx-fn tx-fns)]
    (reduce (fn [acc tx-fn]
              (let [df (make-tx-fn-data-frame tx-fn state)
                    {:keys [f output-path]} tx-fn]
                (assoc-in acc output-path (f df))))
            state xfed-tx-fns)))
