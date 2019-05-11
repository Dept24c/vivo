(ns com.dept24c.vivo.mem-state-provider-impl
  (:require
   [clojure.string :as str]
   [com.dept24c.vivo.state :as state]
   [com.dept24c.vivo.utils :as u]))


(defn get-in-state
  "Custom get-in fn that checks types and normalizes negative keys.
   Returns a map with :norm-path and :val keys."
  [state path]
  (reduce (fn [{:keys [norm-path val] :as acc} k]
            (let [[k* val*] (cond
                              (or (keyword? k) (nat-int? k) (string? k))
                              [k (when val
                                   (val k))]

                              (and (int? k) (neg? k))
                              (let [k (when val
                                        (if (or (vector? val) (nil? val))
                                          (+ (count val) k)
                                          k))]
                                [k (val k)])

                              :else
                              (throw
                               (ex-info
                                (str "Illegal key `" k "` in path `" path
                                     "`. Only integers, keywords, and strings "
                                     "are valid path keys.")
                                (u/sym-map k path))))]
              (-> acc
                  (update :norm-path conj k*)
                  (assoc :val val*))))
          {:norm-path []
           :val state}
          path))

(defn check-one-param [path op upex]
  (when-not (= 2 (count upex))
    (throw (ex-info (str op " update expressions must have exactly "
                         "one parameter, e.g. [" op " 42]. Got `"
                         upex "`.")
                    (u/sym-map path op upex)))))

(defmulti eval-upex (fn [state path [op & args]]
                      op))

(defmethod eval-upex :set
  [state path upex]
  (let [[op new-v] upex
        _ (check-one-param path op upex)
        {:keys [norm-path val]} (get-in-state state path)]
    (assoc-in state norm-path new-v)))

(defmethod eval-upex :remove
  [state path upex]
  (when-not (= 1 (count upex))
    (throw (ex-info (str ":remove update expressions must not have any"
                         "parameters, e.g. [:remove]. Got `" upex "`.")
                    (u/sym-map path upex))))
  (let [parent-path (butlast path)
        k (last path)
        {:keys [norm-path val]} (get-in-state state parent-path)
        new-parent (if (map? val)
                     (dissoc val k)
                     (let [norm-i (if (nat-int? k)
                                    k
                                    (+ (count val) k))
                           [h t] (split-at norm-i val)]
                       (if (nat-int? norm-i)
                         (vec (concat h (rest t)))
                         val)))]
    (if (empty? norm-path)
      new-parent
      (assoc-in state norm-path new-parent))))

(defn insert* [state path upex]
  (let [[op new-v] upex
        _ (check-one-param path op upex)
        parent-path (butlast path)
        i (last path)
        _ (when-not (int? i)
            (throw (ex-info
                    (str "In " op " update expressions, the last element "
                         "of the path must be an integer, e.g. [:x :y -1] "
                         " or [:a :b :c 12]. Got: `" i "`.")
                    (u/sym-map path parent-path i upex op))))
        {:keys [norm-path val]} (get-in-state state parent-path)
        _ (when-not (or (vector? val) (nil? val))
            (throw (ex-info (str "Bad path in " op ". Path `" path "` does not "
                                 "point to a vector. Got: `" val "`.")
                            (u/sym-map op path val upex norm-path))))
        norm-i (if (nat-int? i)
                 i
                 (+ (count val) i))
        split-i (if (= :insert-before op)
                  norm-i
                  (inc norm-i))
        [h t] (split-at split-i val)
        new-t (cons new-v t)
        new-parent (vec (concat h new-t))]
    (if (empty? norm-path)
      new-parent
      (assoc-in state norm-path new-parent))))

(defmethod eval-upex :insert-before
  [state path upex]
  (insert* state path upex))

(defmethod eval-upex :insert-after
  [state path upex]
  (insert* state path upex))

(defn eval-math-op [state path upex op-fn]
  (let [[op param] upex
        _ (check-one-param path op upex)
        {:keys [norm-path val]} (get-in-state state path)
        _ (when-not (number? val)
            (throw (ex-info (str "Can't do math on non-numeric type. "
                                 "Value in state at path `"
                                 path "` is not a number. Got: " val ".")
                            (u/sym-map path upex op param val))))
        _ (when-not (number? param)
            (throw (ex-info (str "Can't do math on non-numeric type. "
                                 "Param `" param "` in update expression `"
                                 upex "` is not a number.")
                            (u/sym-map path upex op param val))))
        new-val (op-fn val param)]
    (assoc-in state norm-path new-val)))

(defmethod eval-upex :+
  [state path upex]
  (eval-math-op state path upex +))

(defmethod eval-upex :-
  [state path upex]
  (eval-math-op state path upex -))

(defmethod eval-upex :*
  [state path upex]
  (eval-math-op state path upex *))

(defmethod eval-upex :/
  [state path upex]
  (eval-math-op state path upex /))

(defmethod eval-upex :mod
  [state path upex]
  (eval-math-op state path upex mod))

(defn make-data-frame [sub-map state *tx-info-str]
  (let [tx-info-str @*tx-info-str]
    (cond-> (reduce-kv (fn [acc df-key path]
                         (let [{:keys [val]} (get-in-state state path)]
                           (assoc acc df-key val)))
                       {} sub-map)
      tx-info-str (assoc :vivo/tx-info-str tx-info-str))))

(defrecord MemStateProvider [*sub-id->sub *state]
  state/IState
  (update-state! [this update-map]
    (let [*tx-info-str (atom nil)
          orig-state @*state
          new-state (reduce ;; Use reduce, not reduce-kv, to enable ordered seqs
                     (fn [acc [path upex]]
                       (if (= :vivo/tx-info-str path)
                         (do
                           (reset! *tx-info-str upex)
                           acc)
                         (try
                           (eval-upex acc path upex)
                           (catch #?(:clj IllegalArgumentException
                                     :cljs js/Error) e
                             (if-not (str/includes?
                                      (u/ex-msg e)
                                      "No method in multimethod 'eval-upex'")
                               (throw e)
                               (throw
                                (ex-info
                                 (str "Invalid operator `" (first upex)
                                      "` in update expression `" upex "`.")
                                 (u/sym-map path upex update-map))))))))
                     orig-state update-map)]
      (reset! *state new-state)
      (doseq [[sub-id {:keys [sub-map update-fn]}] @*sub-id->sub]
        (when (reduce
               (fn [acc sub-path]
                 (let [orig-v (get-in-state orig-state sub-path)
                       new-v (get-in-state new-state sub-path)]
                   (if (= orig-v new-v)
                     acc
                     (reduced true))))
               false (vals sub-map))
          (update-fn (make-data-frame sub-map new-state *tx-info-str))))))

  (subscribe! [this sub-id sub-map update-fn]
    (let [state @*state
          sub (u/sym-map sub-map update-fn)
          *tx-info-str (atom (u/edn->str :vivo/initial-subscription))
          data-frame (make-data-frame sub-map state *tx-info-str)]
      (swap! *sub-id->sub assoc sub-id sub)
      (update-fn data-frame)
      nil))

  (unsubscribe! [this sub-id]
    (swap! *sub-id->sub dissoc sub-id)
    nil))

(defn mem-state-provider [initial-state]
  (let [*sub-id->sub (atom {})
        *state (atom initial-state)]
    (->MemStateProvider *sub-id->sub *state)))
